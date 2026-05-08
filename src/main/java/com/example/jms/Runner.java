package com.example.jms;

import com.example.kafka.KafkaLatencyTest;
import com.example.kafka.KafkaResponseTimeTest;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class Runner implements ApplicationRunner {

    private final JmsTemplate jms;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Spring will automatically inject both the JmsTemplate and KafkaTemplate here
    public Runner(JmsTemplate jms, KafkaTemplate<String, String> kafkaTemplate) {
        this.jms = jms;
        this.jms.setReceiveTimeout(JmsApplication.RECEIVE_TIMEOUT_MS);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // --- JMS Tests (Commented out to focus on Kafka) ---
        // new JmsResponseTimeTest(jms).run();
        // new JmsThroughputTest(jms).run();
        // new JmsLatencyTest(jms).run();

        // --- Kafka Tests ---
//        new KafkaResponseTimeTest(kafkaTemplate).run();
        new KafkaLatencyTest(kafkaTemplate).run();
    }
}