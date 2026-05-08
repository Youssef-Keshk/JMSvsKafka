package com.example.jms;

import jakarta.jms.Message;
import org.springframework.jms.core.JmsTemplate;

import java.util.ArrayList;
import java.util.List;

public class JmsResponseTimeTest {

    private final JmsTemplate jms;

    public JmsResponseTimeTest(JmsTemplate jms) {
        this.jms = jms;
    }

    public void run() throws Exception {
        System.out.println("\n========== RESPONSE TIME TEST ==========");

        // 1: Produce
        System.out.println("Phase 1: Producing " + JmsApplication.NUM_MESSAGES + " messages...");
        List<Long> produceTimes = new ArrayList<>(JmsApplication.NUM_MESSAGES);

        for (int i = 0; i < JmsApplication.NUM_MESSAGES; i++) {
            long start = System.currentTimeMillis();
            jms.convertAndSend(JmsApplication.Q, JmsApplication.PAYLOAD);
            produceTimes.add(System.currentTimeMillis() - start);
        }
        System.out.printf("  Produce median response time : %d ms%n", JmsUtils.median(produceTimes));

        // 2: Refill then Consume
        System.out.println("Phase 2: Draining and refilling queue...");
        JmsUtils.drainQueue(jms);
        JmsUtils.fillQueue(jms, JmsApplication.NUM_MESSAGES);

        System.out.println("Phase 3: Consuming " + JmsApplication.NUM_MESSAGES + " messages...");
        List<Long> consumeTimes = new ArrayList<>(JmsApplication.NUM_MESSAGES);

        for (int i = 0; i < JmsApplication.NUM_MESSAGES; i++) {
            long start = System.currentTimeMillis();
            Message msg = jms.receive(JmsApplication.Q);
            consumeTimes.add(System.currentTimeMillis() - start);
            if (msg == null) System.err.println("  Warning: null message at iteration " + i);
        }
        System.out.printf("  Consume median response time : %d ms%n", JmsUtils.median(consumeTimes));

        System.out.println("=========================================");
    }
}