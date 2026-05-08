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

public class KafkaResponseTimeTest {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public static final String TOPIC = "response-time-topic";
    public static final int NUM_MESSAGES = 1000;
    public static final String PAYLOAD = "A".repeat(1024); // ~1 KB

    public KafkaResponseTimeTest(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void run() throws Exception {
        System.out.println("\n========== KAFKA RESPONSE TIME TEST ==========");

        // 1: Produce Phase
        System.out.println("Phase 1: Producing " + NUM_MESSAGES + " messages synchronously...");
        List<Long> produceTimes = new ArrayList<>(NUM_MESSAGES);

        for (int i = 0; i < NUM_MESSAGES; i++) {
            long start = System.currentTimeMillis();

            // .get() forces the async Kafka producer to wait for the broker's acknowledgment.
            kafkaTemplate.send(TOPIC, PAYLOAD).get();

            produceTimes.add(System.currentTimeMillis() - start);
        }
        System.out.printf("  Produce median response time : %d ms%n", median(produceTimes));


        // 2: Consume Phase
        System.out.println("Phase 2: Consuming " + NUM_MESSAGES + " messages...");
        List<Long> consumeTimes = new ArrayList<>(NUM_MESSAGES);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "response-time-group-" + System.currentTimeMillis()); // Unique group to read from offset 0
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        // Force the consumer to fetch exactly 1 message per API call to measure individual response time
        props.put("max.poll.records", "1");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            int messagesConsumed = 0;
            while (messagesConsumed < NUM_MESSAGES) {
                long start = System.currentTimeMillis();

                // Poll for a single message
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    long responseTime = System.currentTimeMillis() - start;
                    for (ConsumerRecord<String, String> ignored : records) {
                        consumeTimes.add(responseTime);
                        messagesConsumed++;
                    }
                }
            }
        }

        System.out.printf("  Consume median response time : %d ms%n", median(consumeTimes));
        System.out.println("==============================================");
    }

    // Helper method to calculate median
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