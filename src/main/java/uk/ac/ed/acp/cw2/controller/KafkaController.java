package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@RestController
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final Gson gson = new Gson();

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    private Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", UUID.randomUUID().toString());
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "true");

        // Add security settings if configured
        if (environment.getKafkaSecurityProtocol() != null) {
            properties.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            properties.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            properties.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }

        return properties;
    }

    @PutMapping("kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<Void> writeToKafka(@PathVariable String writeTopic, @PathVariable int messageCount) {
        logger.info("Writing {} messages to Kafka topic: {}", messageCount, writeTopic);

        Properties properties = getKafkaProperties();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < messageCount; i++) {
                Map<String, Object> message = new HashMap<>();
                message.put("uid", "s1234567"); // Replace with your student ID
                message.put("counter", i);

                String messageJson = gson.toJson(message);
                ProducerRecord<String, String> record = new ProducerRecord<>(writeTopic, messageJson);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Error sending message to Kafka", exception);
                    } else {
                        logger.info("Message sent to topic: {}, partition: {}, offset: {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
            }

            // Ensure all messages are sent
            producer.flush();
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Error while writing to Kafka", e);
            throw new RuntimeException("Failed to write to Kafka", e);
        }
    }

    @GetMapping("kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> readFromKafka(@PathVariable String readTopic, @PathVariable int timeoutInMsec) {
        logger.info("Reading from Kafka topic: {} with timeout: {}ms", readTopic, timeoutInMsec);

        Properties properties = getKafkaProperties();
        List<String> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            // Set timeout for polling
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutInMsec));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Received message: Topic = {}, Partition = {}, Offset = {}, Key = {}, Value = {}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
                messages.add(record.value());
            }

            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            logger.error("Error while reading from Kafka", e);
            throw new RuntimeException("Failed to read from Kafka", e);
        }
    }
}