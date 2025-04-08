package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@RestController
public class RabbitMqController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private final RuntimeEnvironment environment;
    private final Gson gson = new Gson();

    public RabbitMqController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @PutMapping("rabbitMq/{queueName}/{messageCount}")
    public ResponseEntity<Void> writeToRabbitMq(@PathVariable String queueName, @PathVariable int messageCount) {
        logger.info("Writing {} messages to RabbitMQ queue: {}", messageCount, queueName);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int i = 0; i < messageCount; i++) {
                Map<String, Object> message = new HashMap<>();
                message.put("uid", "s1234567"); // Replace with your student ID
                message.put("counter", i);

                String messageJson = gson.toJson(message);
                channel.basicPublish("", queueName, null, messageJson.getBytes(StandardCharsets.UTF_8));
                logger.info("Sent message {} to queue {}", messageJson, queueName);
            }

            return ResponseEntity.ok().build();
        } catch (IOException | TimeoutException e) {
            logger.error("Error while writing to RabbitMQ", e);
            throw new RuntimeException("Failed to write to RabbitMQ", e);
        }
    }

    @GetMapping("rabbitMq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> readFromRabbitMq(@PathVariable String queueName, @PathVariable int timeoutInMsec) {
        logger.info("Reading from RabbitMQ queue: {} with timeout: {}ms", queueName, timeoutInMsec);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        List<String> messages = new ArrayList<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            // Set the end time for polling
            long endTime = System.currentTimeMillis() + timeoutInMsec;

            while (System.currentTimeMillis() < endTime) {
                GetResponse response = channel.basicGet(queueName, true);
                if (response == null) {
                    // No message available, wait a bit and try again
                    Thread.sleep(100);
                    continue;
                }

                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                logger.info("Received message: {}", message);
                messages.add(message);
            }

            // Ensure we don't exceed the timeout by more than 200ms
            if (System.currentTimeMillis() > endTime + 200) {
                logger.warn("Exceeded timeout by more than 200ms");
            }

            return ResponseEntity.ok(messages);
        } catch (IOException | TimeoutException | InterruptedException e) {
            logger.error("Error while reading from RabbitMQ", e);
            throw new RuntimeException("Failed to read from RabbitMQ", e);
        }
    }
}