package com.example.jms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class JmsApplication {

	public static void main(String[] args) {
		SpringApplication.run(JmsApplication.class, args);
	}

	static final String Q = "message";
	static final int NUM_MESSAGES = 1000;
	static final String PAYLOAD = "A".repeat(1024); // ~1 KB
	static final int RECEIVE_TIMEOUT_MS = 5000;
	static final int DRAIN_TIMEOUT_MS = 300;
}