package com.dataweave.kafka;

import com.dataweave.avro.Tool;
import com.dataweave.avro.User;
import com.dataweave.dto.request.LoginRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;


@Slf4j
@Service
public class KafkaAvroProducerService {

    private final KafkaTemplate<String, User> avroKafkaTemplate;
    private ThreadPoolTaskExecutor kafkaAvroProducerExecutor;
    private final int workerThreadCount;
    private final double logProbability;
    private Counter eventPublishedCounter;
    private Counter eventFailedCounter;


    private void initMetrics(MeterRegistry meterRegistry) {
        String serializationType = "avro"; //System.getenv().get("SERIALIZATION_TYPE");
        if (serializationType == null || serializationType.isEmpty()) {
            throw new RuntimeException("serializationType is invalid");
        }

        this.eventPublishedCounter =
                Counter.builder("kafka_events_published_total")
                        .description("Total Kafka events published (Avro)")
                        .tag("serialization", serializationType)
                        .register(meterRegistry);

        this.eventFailedCounter =
                Counter.builder("kafka_events_failed_total")
                        .description("Total Kafka events failed to publish (Avro)")
                        .tag("serialization", "avro")
                        .register(meterRegistry);
    }


    private void initThreadPoolExecutor(MeterRegistry meterRegistry, int workerThreadCount) {
        this.kafkaAvroProducerExecutor = new ThreadPoolTaskExecutor();
        this.kafkaAvroProducerExecutor.setCorePoolSize(workerThreadCount);
        this.kafkaAvroProducerExecutor.setMaxPoolSize(workerThreadCount);
        this.kafkaAvroProducerExecutor.setThreadNamePrefix("kafka-avro-producer-");
        this.kafkaAvroProducerExecutor.initialize();
        ExecutorServiceMetrics.monitor(
                meterRegistry,
                this.kafkaAvroProducerExecutor.getThreadPoolExecutor(),
                "kafka-avro-producer-executor");
    }


    public KafkaAvroProducerService(
            KafkaTemplate<String, User> avroKafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${kafka.producer.worker-count}") int workerThreadCount,
            @Value("${log.probability}") double logProbability) {
        this.avroKafkaTemplate = avroKafkaTemplate;
        this.workerThreadCount = workerThreadCount;
        this.logProbability = logProbability;
        initThreadPoolExecutor(meterRegistry, workerThreadCount);
        initMetrics(meterRegistry);
    }


    /**
     * Converts the generic Map payload (from LoginRequest) into a User Avro object.
     */
    private User convertToUser(Map<String, Object> payload, String eventId) {
        User.Builder builder = User.newBuilder();

        // Required fields
        builder.setEventId(eventId);
        builder.setTimestamp(System.currentTimeMillis());
        builder.setIsActive((Boolean) payload.getOrDefault("isActive", false));
        builder.setPicture((String) payload.getOrDefault("picture", ""));
        builder.setAge(payload.getOrDefault("age", 0) instanceof Number
                ? ((Number) payload.get("age")).intValue() : 0);
        builder.setStatus((String) payload.getOrDefault("status", ""));
        builder.setName((String) payload.getOrDefault("name", ""));
        builder.setAddress((String) payload.getOrDefault("address", ""));

        // about array
        Object aboutObj = payload.get("about");
        List<String> aboutList = new ArrayList<>();
        if (aboutObj instanceof List) {
            aboutList = ((List<?>) aboutObj).stream()
                    .filter(o -> o instanceof String)
                    .map(String::valueOf)
                    .collect(Collectors.toList());
        }
        builder.setAbout(aboutList);

        builder.setLatitude(payload.getOrDefault("latitude", 0.0) instanceof Number
                ? ((Number) payload.get("latitude")).doubleValue() : 0.0);
        builder.setLongitude(payload.getOrDefault("longitude", 0.0) instanceof Number
                ? ((Number) payload.get("longitude")).doubleValue() : 0.0);

        // tools array
        Object toolsObj = payload.get("tools");
        List<Tool> toolsList = new ArrayList<>();
        if (toolsObj instanceof List) {
            toolsList = ((List<?>) toolsObj).stream()
                    .filter(item -> item instanceof Map)
                    .map(item -> {
                        Map<?, ?> map = (Map<?, ?>) item;
                        String toolId = map.get("id") == null ? "" : map.get("id").toString();
                        String toolName = map.get("name") == null ? "" : map.get("name").toString();
                        return Tool.newBuilder().setId(toolId).setName(toolName).build();
                    })
                    .collect(Collectors.toList());
        }
        builder.setTools(toolsList);
        return builder.build();
    }


    private void publish(String topic, User user) {
        boolean shouldLog = ThreadLocalRandom.current().nextDouble() < this.logProbability;

        avroKafkaTemplate.send(topic, null, user)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Avro Kafka publish failed", ex);
                        eventFailedCounter.increment();
                    } else {
                        if (shouldLog) {
                            log.info("Avro Kafka publish success, size in bytes : [{}]", result.getRecordMetadata().serializedValueSize());
                        }
                        eventPublishedCounter.increment();
                    }
                });
    }


    /**
     * Publishes Avro messages asynchronously based on the LoginRequest.
     * The request contains a jsonPayload (Map) that must conform to the User Avro schema.
     */
    public void publishAvroMessagesAsync(LoginRequest req) {
        long messagesPerThread = (long) Math.ceil((double) req.getEventCount() / workerThreadCount);
        if (messagesPerThread < 1) {
            throw new RuntimeException("messagesPerThread must be at least 1");
        }

        for (int thread = 0; thread < workerThreadCount; thread++) {
            int finalThread = thread;
            kafkaAvroProducerExecutor.submit(() -> {
                for (long i = 0; i < messagesPerThread; i++) {
                    // Clone the payload map from the request
                    // Map<String, Object> eventPayload = new HashMap<>(req.getJsonPayload());

                    @SuppressWarnings("unchecked")
                    Map<String, Object> eventPayload =
                            new HashMap<>((Map<String, Object>) req.getJsonPayload());

                    String eventId = req.getBatchId() + "_" + finalThread + "_[" + i + "/" + messagesPerThread + "]";

                    // Convert the map to an Avro User object
                    User user = convertToUser(eventPayload, eventId);

                    // Send to Kafka (topic from request)
                    publish(req.getTopic(), user);

                    // Sleep if required
                    try {
                        Thread.sleep(req.getSleepMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
        }
    }


    @PreDestroy
    public void shutdown() {
        if (kafkaAvroProducerExecutor != null) {
            kafkaAvroProducerExecutor.shutdown();
        }
    }
}