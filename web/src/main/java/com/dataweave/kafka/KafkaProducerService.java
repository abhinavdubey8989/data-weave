package com.dataweave.kafka;


import com.dataweave.dto.request.LoginRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private ThreadPoolTaskExecutor kafkaProducerExecutor;
    private final int workerThreadCount;
    private Counter eventPublishedCounter;
    private Counter eventFailedCounter;
    private final double logProbability;


    private void initMetrics(MeterRegistry meterRegistry) {
        String serializationType = "json"; //System.getenv().get("SERIALIZATION_TYPE");
        if (serializationType == null || serializationType.isEmpty()) {
            throw new RuntimeException("serializationType is invalid");
        }

        this.eventPublishedCounter =
                Counter.builder("kafka_events_published_total")
                        .description("Total Kafka events published")
                        .tag("serialization", "json")
                        .register(meterRegistry);

        this.eventFailedCounter =
                Counter.builder("kafka_events_failed_total")
                        .description("Total Kafka events falied to publish")
                        .tag("serialization", serializationType)
                        .register(meterRegistry);
    }


    private void initThreadPoolExecutor(MeterRegistry meterRegistry, int workerThreadCount) {
        // Create and configure ThreadPoolTaskExecutor
        this.kafkaProducerExecutor = new ThreadPoolTaskExecutor();
        this.kafkaProducerExecutor.setCorePoolSize(workerThreadCount);
        this.kafkaProducerExecutor.setMaxPoolSize(workerThreadCount);
        this.kafkaProducerExecutor.setThreadNamePrefix("kafka-producer-");
        this.kafkaProducerExecutor.initialize();
        // Register executor metrics with Micrometer
        ExecutorServiceMetrics.monitor(
                meterRegistry,
                this.kafkaProducerExecutor.getThreadPoolExecutor(),
                "kafka-producer-executor");
    }


    public KafkaProducerService(
            KafkaTemplate<String, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${kafka.producer.worker-count}")
            int workerThreadCount,
            @Value("${log.probability}")
            double logProbability) {
        this.kafkaTemplate = kafkaTemplate;
        this.workerThreadCount = workerThreadCount;
        this.logProbability = logProbability;
        initThreadPoolExecutor(meterRegistry, workerThreadCount);
        this.initMetrics(meterRegistry);
    }


    private void publish(String topic, Object event) {
        boolean shouldLog = ThreadLocalRandom.current().nextDouble() < this.logProbability;

        kafkaTemplate.send(topic,
                        null,
                        event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Kafka publish failed",
                                ex);
                        eventFailedCounter.increment();
                    } else {
                        if (shouldLog) {
                            log.info("json Kafka publish success, size in bytes : [{}]", result.getRecordMetadata().serializedValueSize());
                        }
                        eventPublishedCounter.increment();
                    }
                });
    }


    /**
     *
     * @param req
     * [sleep in millisecond after 1 event] - [threads] - [time take to publish 1 million events]
     * 1ms -  1 - 17 minutes
     * 5ms -  1 - 85 minutes
     * 10ms - 1 - 170 minutes
     *
     * 1ms   - 4 - 5 minutes
     * 5ms   - 4 - 22 minutes
     * 10ms  - 4 - 43 minutes
     *
     * Tip : use 4 threads & 5 milli-second sleep
     *
     */
    public void publishMessagesAsync(LoginRequest req) {
        long messagesPerThread = (long) Math.ceil((double) req.getEventCount() / workerThreadCount);

        if (messagesPerThread < 1) {
            throw new RuntimeException(
                    "messagesPerThread must be at least 1");
        }

        for (int thread = 0; thread < workerThreadCount; thread++) {
            int finalThread = thread;
            kafkaProducerExecutor.submit(() -> {
                for (long i = 0; i < messagesPerThread; i++) {
                    Map<String, Object> eventPayload =
                            new HashMap<>(
                                    (Map<String, Object>) req.getJsonPayload());
                    String eventId = req.getBatchId() + "_" + finalThread + "_[" + i + "/" + messagesPerThread + "]";

                    // add an event-id
                    eventPayload.put(
                            "eventId",
                            eventId);

                    eventPayload.put(
                            "timestamp",
                            System.currentTimeMillis());

                    // send to kafka
                    publish(req.getTopic(), eventPayload);

                    // sleep
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
        if (kafkaProducerExecutor != null) {
            kafkaProducerExecutor.shutdown();
        }
    }
}