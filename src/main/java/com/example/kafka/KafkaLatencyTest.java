package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaLatencyTest {

    private final KafkaTemplate<String, String> kafkaTemplate;
    public static final String TOPIC = "latency-topic";
    public static final int LATENCY_MESSAGES = 10000;
    public static final String PAYLOAD = "A".repeat(1024);

    public KafkaLatencyTest(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void run() throws Exception {
        System.out.println("\n========== KAFKA LATENCY TEST ==========");

        // WARM UP: Send a single message synchronously to ensure the topic is created
        // and the producer has fetched the cluster metadata before we start timing.
        kafkaTemplate.send(TOPIC, "WARMUP").get();

        System.out.println("Starting concurrent producer and active consumer for " + LATENCY_MESSAGES + " messages...");

        List<Long> latencies = Collections.synchronizedList(new ArrayList<>(LATENCY_MESSAGES));
        CountDownLatch consumerReady = new CountDownLatch(1);
        CountDownLatch testComplete = new CountDownLatch(LATENCY_MESSAGES);

        // 1. Start the Consumer Thread
        Thread consumerThread = new Thread(() -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "latency-group-" + System.currentTimeMillis());
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC));

                // THE FIX: Loop until Kafka explicitly assigns a partition to this consumer
                while (consumer.assignment().isEmpty()) {
                    consumer.poll(Duration.ofMillis(100));
                }

                // Force the consumer to ignore old messages and only look for brand new ones
                consumer.seekToEnd(consumer.assignment());

                consumer.assignment().forEach(consumer::position);

                consumerReady.countDown(); // Consumer is locked in. Signal the producer!

                while (testComplete.getCount() > 0) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    long receiveTime = System.currentTimeMillis();

                    for (ConsumerRecord<String, String> record : records) {
                        long sendTime = record.timestamp();
                        latencies.add(receiveTime - sendTime);
                        testComplete.countDown();
                    }
                }
            }
        });

        // 2. Start the Producer Thread
        Thread producerThread = new Thread(() -> {
            try {
                // Wait securely until the consumer says it is actively listening
                consumerReady.await();

                for (int i = 0; i < LATENCY_MESSAGES; i++) {
                    kafkaTemplate.send(TOPIC, PAYLOAD);
                }

                kafkaTemplate.flush();

            } catch (Exception e) {
                System.err.println("Producer error: " + e.getMessage());
            }
        });

        consumerThread.start();
        producerThread.start();

        // Safety timeout
        boolean finished = testComplete.await(15, TimeUnit.SECONDS);
        if (!finished) {
            System.err.println("  Warning: Timed out waiting. Only received " + latencies.size() + " messages.");
        }

        consumerThread.join();
        producerThread.join();

        // 3. Calculate and report
        long medianLatency = median(latencies);
        System.out.printf("  Successfully processed %d messages.%n", latencies.size());
        System.out.printf("  Median Latency : %d ms%n", medianLatency);

        System.out.println("========================================");
    }

    private long median(List<Long> values) {
        if (values.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int mid = sorted.size() / 2;
        return sorted.size() % 2 == 0
                ? (sorted.get(mid - 1) + sorted.get(mid)) / 2
                : sorted.get(mid);
    }
}