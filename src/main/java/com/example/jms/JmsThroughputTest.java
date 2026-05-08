package com.example.jms;

import jakarta.jms.Message;
import org.springframework.jms.core.JmsTemplate;

import java.util.concurrent.atomic.AtomicInteger;

public class JmsThroughputTest {

    private static final int WINDOW_MS  = 1000; // 1-second test window
    private static final int START_X    = 10;   // starting throughput (msg/sec)
//    private static final int STEP_DOWN  = 2;   // decrement step after a failure

    private final JmsTemplate jms;

    public JmsThroughputTest(JmsTemplate jms) {
        this.jms = jms;
    }

    public void run() throws Exception {
        System.out.println("\n========== THROUGHPUT TEST ==========");

        int maxProduce = findMaxThroughput("Produce", this::runProduceRound, true);
        System.out.printf("%nMax PRODUCE throughput : %d msg/sec%n", maxProduce);

        int maxConsume = findMaxThroughput("Consume", this::runConsumeRound, false);
        System.out.printf("%nMax CONSUME throughput : %d msg/sec%n", maxConsume);

        System.out.println("=====================================");
    }

    // ── Generic finder ────────────────────────────────────────────────────

    /**
     * Phase 1 — exponential climb until first failure.
     * Phase 2 — step down by STEP_DOWN from the failed value until success.
     */
    private int findMaxThroughput(String label, RoundRunner runner, boolean isProduce) {
        System.out.println("\n--- " + label + " Throughput ---");
        if (isProduce) JmsUtils.drainQueue(jms);

        // ── Phase 1: exponential climb ────────────────────────────────
        int x = START_X, lastSuccess = 0;
        while (true) {
            if (!isProduce) JmsUtils.fillQueue(jms, x);
            boolean ok = runner.run(x);
            if (isProduce) JmsUtils.drainQueue(jms); else JmsUtils.drainQueue(jms);
            System.out.printf("  X = %6d msg/sec  →  %s%n", x, ok ? "OK" : "FAILED");

            if (ok) {
                lastSuccess = x;
                x *= 2;
                if (x > 1_000_000) { return lastSuccess; } // safety ceiling
            } else {
                break;
            }
        }

//        // ── Phase 2: step down from failed x to find true max ────────
//        // Start just below the failed value and decrement by STEP_DOWN
//        int stepX = x - STEP_DOWN;
//        while (stepX > lastSuccess && stepX > 0) {
//            if (!isProduce) JmsUtils.fillQueue(jms, stepX);
//            boolean ok = runner.run(stepX);
//            if (isProduce) JmsUtils.drainQueue(jms); else JmsUtils.drainQueue(jms);
//            System.out.printf("  X = %6d msg/sec  →  %s  (refinement)%n", stepX, ok ? "OK" : "FAILED");
//
//            if (ok) {
//                return stepX; // first success in step-down = true max
//            }
//            stepX -= STEP_DOWN;
//        }

        return lastSuccess; // fallback to last exponential success
    }

    // ── Produce round ─────────────────────────────────────────────────────

    private boolean runProduceRound(int x) {
        double T         = (double) WINDOW_MS / x;
        long   windowEnd = System.currentTimeMillis() + WINDOW_MS;
        AtomicInteger sent = new AtomicInteger(), errors = new AtomicInteger();

        for (int i = 0; i < x; i++) {
            if (System.currentTimeMillis() >= windowEnd) {
                errors.incrementAndGet();
                break;
            }

            long sendStart = System.currentTimeMillis();
            try {
                jms.convertAndSend(JmsApplication.Q, JmsApplication.PAYLOAD);
                sent.incrementAndGet();
            } catch (Exception e) {
                errors.incrementAndGet();
            }

            // Sleep = 0.8*T minus actual time already spent sending
            long elapsed = System.currentTimeMillis() - sendStart;
            long sleepMs = (long) (0.8 * T) - elapsed;
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); } catch (InterruptedException ignored) {}
            }
        }

        return errors.get() == 0 && sent.get() == x;
    }

    // ── Consume round ─────────────────────────────────────────────────────

    private boolean runConsumeRound(int x) {
        double T         = (double) WINDOW_MS / x;
        long   windowEnd = System.currentTimeMillis() + WINDOW_MS;
        AtomicInteger received = new AtomicInteger(), errors = new AtomicInteger();

        for (int i = 0; i < x; i++) {
            if (System.currentTimeMillis() >= windowEnd) {
                errors.incrementAndGet();
                break;
            }

            long recvStart = System.currentTimeMillis();
            try {
                Message msg = jms.receive(JmsApplication.Q);
                if (msg == null) errors.incrementAndGet();
                else received.incrementAndGet();
            } catch (Exception e) {
                errors.incrementAndGet();
            }

            long elapsed = System.currentTimeMillis() - recvStart;
            long sleepMs = (long) (0.8 * T) - elapsed;
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); } catch (InterruptedException ignored) {}
            }
        }

        return errors.get() == 0 && received.get() == x;
    }

    // ── Functional interface for round runners ────────────────────────────

    @FunctionalInterface
    interface RoundRunner {
        boolean run(int x);
    }
}