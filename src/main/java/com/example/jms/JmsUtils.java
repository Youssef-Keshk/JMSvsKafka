package com.example.jms;

import org.springframework.jms.core.JmsTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JmsUtils {

    /** Drain every message from the queue using a short receive timeout. */
    public static void drainQueue(JmsTemplate jms) {
        jms.setReceiveTimeout(JmsApplication.DRAIN_TIMEOUT_MS);
        while (jms.receive(JmsApplication.Q) != null) {}
        jms.setReceiveTimeout(JmsApplication.RECEIVE_TIMEOUT_MS);
    }

    /** Fill the queue with {@code count} ~1KB messages. */
    public static void fillQueue(JmsTemplate jms, int count) {
        for (int i = 0; i < count; i++) {
            jms.convertAndSend(JmsApplication.Q, JmsApplication.PAYLOAD);
        }
    }

    /** Returns the median of a list of longs. */
    public static long median(List<Long> values) {
        if (values.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int mid = sorted.size() / 2;
        return sorted.size() % 2 == 0
                ? (sorted.get(mid - 1) + sorted.get(mid)) / 2
                : sorted.get(mid);
    }
}