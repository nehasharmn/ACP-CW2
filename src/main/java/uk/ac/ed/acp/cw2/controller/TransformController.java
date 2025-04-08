package uk.ac.ed.acp.cw2.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.model.TransformMessagesRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RestController
public class TransformController {

    private static final Logger logger = LoggerFactory.getLogger(TransformController.class);
    private final RuntimeEnvironment environment;
    private final Gson gson = new Gson();

    public TransformController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    @PostMapping("transformMessages")
    public ResponseEntity<Void> transformMessages(@RequestBody TransformMessagesRequest request) {
        logger.info("Transforming messages: readQueue={}, writeQueue={}, messageCount={}",
                request.getReadQueue(), request.getWriteQueue(), request.getMessageCount());

        // Initialize Redis and RabbitMQ connections
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        JedisPool jedisPool = new JedisPool(environment.getRedisHost(), environment.getRedisPort());

        // Statistics counters
        AtomicInteger totalMessagesWritten = new AtomicInteger(0);
        AtomicInteger totalMessagesProcessed = new AtomicInteger(0);
        AtomicInteger totalRedisUpdates = new AtomicInteger(0);
        AtomicReference<Double> totalValueWritten = new AtomicReference<>(0.0);
        AtomicReference<Double> totalAdded = new AtomicReference<>(0.0);

        try (Connection connection = factory.newConnection();
             Channel readChannel = connection.createChannel();
             Channel writeChannel = connection.createChannel();
             Jedis jedis = jedisPool.getResource()) {

            // Declare queues
            readChannel.queueDeclare(request.getReadQueue(), false, false, false, null);
            writeChannel.queueDeclare(request.getWriteQueue(), false, false, false, null);

            // Process messages
            int processedCount = 0;

            while (processedCount < request.getMessageCount()) {
                GetResponse response = readChannel.basicGet(request.getReadQueue(), true);

                if (response == null) {
                    // No message available, wait and try again
                    Thread.sleep(100);
                    continue;
                }

                String messageBody = new String(response.getBody(), StandardCharsets.UTF_8);
                totalMessagesProcessed.incrementAndGet();
                processedCount++;

                logger.info("Processing message: {}", messageBody);
                JsonObject jsonMessage = JsonParser.parseString(messageBody).getAsJsonObject();

                // Check if it's a normal message or tombstone
                if (jsonMessage.has("key") && jsonMessage.has("version") && jsonMessage.has("value")) {
                    // Normal message
                    String key = jsonMessage.get("key").getAsString();
                    int version = jsonMessage.get("version").getAsInt();
                    double value = jsonMessage.get("value").getAsDouble();

                    // Create Redis key from message key and version
                    String redisKey = key + "_v" + version;

                    // Check if key in this specific version is already in Redis
                    boolean keyExists = jedis.exists(redisKey);

                    // If not present or older version, store and modify
                    if (!keyExists) {
                        boolean olderVersionExists = false;

                        // Check for older versions
                        for (int v = 1; v < version; v++) {
                            String olderKey = key + "_v" + v;
                            if (jedis.exists(olderKey)) {
                                olderVersionExists = true;
                                break;
                            }
                        }

                        if (!keyExists || olderVersionExists) {
                            // Store entry in Redis
                            jedis.set(redisKey, String.valueOf(value));
                            totalRedisUpdates.incrementAndGet();

                            // Modify value and write to output queue
                            double newValue = value + 10.5;
                            jsonMessage.addProperty("value", newValue);
                            totalValueWritten.updateAndGet(current -> current + newValue);
                            totalAdded.updateAndGet(current -> current + 10.5);
                        } else {
                            // Pass message unmodified
                            totalValueWritten.updateAndGet(current -> current + value);
                        }
                    } else {
                        // Key exists with this version, pass message unmodified
                        totalValueWritten.updateAndGet(current -> current + value);
                    }

                    // Write to output queue
                    writeChannel.basicPublish("", request.getWriteQueue(), null,
                            jsonMessage.toString().getBytes(StandardCharsets.UTF_8));
                    totalMessagesWritten.incrementAndGet();
                } else if (jsonMessage.has("key") && !jsonMessage.has("version") && !jsonMessage.has("value")) {
                    // Tombstone message
                    String key = jsonMessage.get("key").getAsString();

                    // Remove all versions of this key from Redis
                    Set<String> keysToRemove = jedis.keys(key + "_v*");
                    for (String keyToRemove : keysToRemove) {
                        jedis.del(keyToRemove);
                    }

                    // Create special statistics packet
                    JsonObject statsMessage = new JsonObject();
                    statsMessage.addProperty("totalMessagesWritten", totalMessagesWritten.get());
                    statsMessage.addProperty("totalMessagesProcessed", totalMessagesProcessed.get());
                    statsMessage.addProperty("totalRedisUpdates", totalRedisUpdates.get());
                    statsMessage.addProperty("totalValueWritten", totalValueWritten.get());
                    statsMessage.addProperty("totalAdded", totalAdded.get());

                    // Write statistics to output queue
                    writeChannel.basicPublish("", request.getWriteQueue(), null,
                            statsMessage.toString().getBytes(StandardCharsets.UTF_8));
                } else {
                    logger.warn("Received message with unexpected format: {}", messageBody);
                }
            }

            return ResponseEntity.ok().build();
        } catch (IOException | TimeoutException | InterruptedException e) {
            logger.error("Error transforming messages", e);
            throw new RuntimeException("Failed to transform messages", e);
        } finally {
            jedisPool.close();
        }
    }
}