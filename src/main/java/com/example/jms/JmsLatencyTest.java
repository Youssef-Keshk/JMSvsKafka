package com.example.jms;

import jakarta.jms.Message;
import org.springframework.jms.core.JmsTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class JmsLatencyTest {

    private final JmsTemplate jms;
    private static final int LATENCY_MESSAGES = 10000;

    public JmsLatencyTest(JmsTemplate jms) {
        this.jms = jms;
    }

    public void run() throws Exception {
        System.out.println("\n========== LATENCY TEST ==========");

        // Ensure the queue is empty before starting
        System.out.println("Draining queue...");
        JmsUtils.drainQueue(jms);

        System.out.println("Starting concurrent producer and consumer for " + LATENCY_MESSAGES + " messages...");

        List<Long> latencies = new ArrayList<>(LATENCY_MESSAGES);
        CountDownLatch consumerReady = new CountDownLatch(1);

        // 1. Start the Consumer Thread
        Thread consumerThread = new Thread(() -> {
            consumerReady.countDown(); // Signal that the consumer is running
            for (int i = 0; i < LATENCY_MESSAGES; i++) {
                try {
                    Message msg = jms.receive(JmsApplication.Q);
                    long receiveTime = System.currentTimeMillis();

                    if (msg != null) {
                        // Extract the timestamp injected by the producer
                        long sendTime = msg.getLongProperty("sendTimestamp");
                        latencies.add(receiveTime - sendTime);
                    } else {
                        System.err.println("  Warning: Received null message (timeout). Aborting consumer loop.");
                        break;
                    }
                } catch (Exception e) {
                    System.err.println("  Error during consumption: " + e.getMessage());
                    break;
                }
            }
        });

        // 2. Start the Producer Thread
        Thread producerThread = new Thread(() -> {
            try {
                // Wait briefly to ensure the consumer thread is actively listening
                consumerReady.await();

                for (int i = 0; i < LATENCY_MESSAGES; i++) {
                    // Send message and attach current timestamp as a property
                    jms.convertAndSend(JmsApplication.Q, JmsApplication.PAYLOAD, message -> {
                        message.setLongProperty("sendTimestamp", System.currentTimeMillis());
                        return message;
                    });
                }
            } catch (Exception e) {
                System.err.println("  Error during production: " + e.getMessage());
            }
        });

        // Execute threads
        consumerThread.start();
        producerThread.start();

        // Wait for both threads to finish
        producerThread.join();
        consumerThread.join();

        // 3. Calculate and report the median latency
        long medianLatency = JmsUtils.median(latencies);
        System.out.printf("  Successfully processed %d messages.%n", latencies.size());
        System.out.printf("  Median Latency : %d ms%n", medianLatency);

        System.out.println("==================================");
    }
}