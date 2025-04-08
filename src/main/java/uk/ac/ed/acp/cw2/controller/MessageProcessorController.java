package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.model.ProcessMessagesRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@RestController
public class MessageProcessorController {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessorController.class);
    private final RuntimeEnvironment environment;
    private final Gson gson = new Gson();
    private final RestTemplate restTemplate = new RestTemplate();
    private final String studentId = "s1234567"; // Replace with your student ID

    public MessageProcessorController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    private Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", environment.getKafkaBootstrapServers());
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

    @PostMapping("processMessages")
    public ResponseEntity<Void> processMessages(@RequestBody ProcessMessagesRequest request) {
        logger.info("Processing messages: readTopic={}, writeQueueGood={}, writeQueueBad={}, messageCount={}",
                request.getReadTopic(), request.getWriteQueueGood(), request.getWriteQueueBad(), request.getMessageCount());

        // Initialize RabbitMQ
        ConnectionFactory rabbitFactory = new ConnectionFactory();
        rabbitFactory.setHost(environment.getRabbitMqHost());
        rabbitFactory.setPort(environment.getRabbitMqPort());

        // Initialize Kafka
        Properties kafkaProps = getKafkaProperties();

        // Track the running total for good messages
        AtomicReference<Double> runningTotalValue = new AtomicReference<>(0.0);
        AtomicReference<Double> badQueueTotal = new AtomicReference<>(0.0);
        int goodCount = 0;
        int badCount = 0;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
             Connection rabbitConnection = rabbitFactory.newConnection();
             Channel goodChannel = rabbitConnection.createChannel();
             Channel badChannel = rabbitConnection.createChannel()) {

            // Declare RabbitMQ queues
            goodChannel.queueDeclare(request.getWriteQueueGood(), false, false, false, null);
            badChannel.queueDeclare(request.getWriteQueueBad(), false, false, false, null);

            // Subscribe to Kafka topic and reset to beginning
            consumer.subscribe(Collections.singletonList(request.getReadTopic()));
            consumer.poll(Duration.ofMillis(100)); // Initial poll to get assignment
            consumer.seekToBeginning(consumer.assignment());

            int processedCount = 0;

            while (processedCount < request.getMessageCount()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if (processedCount >= request.getMessageCount()) {
                        break;
                    }

                    try {
                        String messageValue = record.value();
                        logger.info("Processing Kafka message: {}", messageValue);

                        JsonObject jsonMessage = JsonParser.parseString(messageValue).getAsJsonObject();

                        // Add null checks for each field
                        JsonElement uidElement = jsonMessage.get("uid");
                        JsonElement keyElement = jsonMessage.get("key");
                        JsonElement commentElement = jsonMessage.get("comment");
                        JsonElement valueElement = jsonMessage.get("value");

                        // Skip if any required field is missing
                        if (uidElement == null || keyElement == null || commentElement == null || valueElement == null) {
                            logger.error("Message is missing required fields: {}", messageValue);
                            continue; // Skip this message and move to the next
                        }

                        String uid = uidElement.getAsString();
                        String key = keyElement.getAsString();
                        String comment = commentElement.getAsString();
                        double value = valueElement.getAsDouble();

                        if (key.length() == 3 || key.length() == 4) {
                            // Good message
                            // Update running total
                            runningTotalValue.updateAndGet(current -> current + value);

                            // Add running total to JSON
                            jsonMessage.addProperty("runningTotalValue", runningTotalValue.get());

                            // Store in ACP Storage Service
                            String storageUrl = environment.getAcpStorageService() + "/api/v1/blob";
                            String uuid = restTemplate.postForObject(storageUrl, jsonMessage.toString(), String.class);

                            // Add UUID to message
                            jsonMessage.addProperty("uuid", uuid);

                            // Write to good queue
                            goodChannel.basicPublish("", request.getWriteQueueGood(), null,
                                    jsonMessage.toString().getBytes(StandardCharsets.UTF_8));
                            goodCount++;
                        } else {
                            // Bad message (key length is 5 or other)
                            badChannel.basicPublish("", request.getWriteQueueBad(), null,
                                    messageValue.getBytes(StandardCharsets.UTF_8));
                            badCount++;
                            badQueueTotal.updateAndGet(current -> current + value);
                        }

                        processedCount++;
                    } catch (Exception e) {
                        logger.error("Error processing message: {}", record.value(), e);
                        // Continue with the next message
                    }
                }
            }

            // After processing all messages, write TOTAL messages to both queues
            // TOTAL message for good queue
            JsonObject goodTotalMessage = new JsonObject();
            goodTotalMessage.addProperty("uid", studentId);
            goodTotalMessage.addProperty("key", "TOTAL");
            goodTotalMessage.addProperty("comment", "");
            goodTotalMessage.addProperty("value", runningTotalValue.get());

            goodChannel.basicPublish("", request.getWriteQueueGood(), null,
                    goodTotalMessage.toString().getBytes(StandardCharsets.UTF_8));

            // TOTAL message for bad queue
            JsonObject badTotalMessage = new JsonObject();
            badTotalMessage.addProperty("uid", studentId);
            badTotalMessage.addProperty("key", "TOTAL");
            badTotalMessage.addProperty("comment", "");
            badTotalMessage.addProperty("value", badQueueTotal.get());

            badChannel.basicPublish("", request.getWriteQueueBad(), null,
                    badTotalMessage.toString().getBytes(StandardCharsets.UTF_8));

            logger.info("Completed processing messages. Good: {}, Bad: {}, Total Good Value: {}, Total Bad Value: {}",
                    goodCount, badCount, runningTotalValue.get(), badQueueTotal.get());

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Error processing messages", e);
            throw new RuntimeException("Failed to process messages", e);
        }
    }
}