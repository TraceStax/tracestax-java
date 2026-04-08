package com.tracestax;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class TraceStaxClientTest {

    private static final String INGEST_URL =
        System.getenv().getOrDefault("TRACESTAX_INGEST_URL", "http://localhost:4001");

    // ── Unit tests: disabled/dry-run mode ────────────────────────────────

    @Test
    void disabledClientDoesNotThrow() {
        TraceStaxClient client = new TraceStaxClient("test-key", "https://localhost:9999", false, false);
        assertDoesNotThrow(() -> {
            client.trackStart("MyJob", "id-1", "default");
            client.trackSuccess("id-1", 123L);
            client.trackFailure("id-1", 123L, new RuntimeException("boom"));
        });
        client.shutdown();
    }

    @Test
    void dryRunClientDoesNotThrow() {
        TraceStaxClient client = new TraceStaxClient("test-key", "https://localhost:9999", true, true);
        assertDoesNotThrow(() -> {
            client.trackStart("MyJob", "id-1", "default");
            client.trackSuccess("id-1", 42L);
        });
        client.shutdown();
    }

    @Test
    void defaultFrameworkIsGeneric() {
        TraceStaxClient client = new TraceStaxClient("test-key", "https://localhost:9999", false, false);
        assertEquals("generic", client.getFramework());
        client.shutdown();
    }

    @Test
    void setFrameworkUpdatesFramework() {
        TraceStaxClient client = new TraceStaxClient("test-key", "https://localhost:9999", false, false);
        client.setFramework("spring_batch");
        assertEquals("spring_batch", client.getFramework());
        client.shutdown();
    }

    // ── Unit tests: local mock server ────────────────────────────────────

    @Test
    void trackStartSendsStartedEvent() throws Exception {
        AtomicInteger hitCount = new AtomicInteger(0);
        ConcurrentLinkedQueue<String> bodies = new ConcurrentLinkedQueue<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/ingest", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            bodies.add(body);
            hitCount.incrementAndGet();
            byte[] resp = "{\"received\":1}".getBytes();
            exchange.sendResponseHeaders(202, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
        });
        server.start();

        String url = "http://localhost:" + server.getAddress().getPort();
        TraceStaxClient client = new TraceStaxClient("ts_test_abc", url, true, false);
        client.setFramework("spring_batch");

        client.trackStart("ProcessOrdersJob", "run-001", "default");

        // Wait for the event to be sent (fire-and-forget with small delay)
        long deadline = System.currentTimeMillis() + 3000;
        while (hitCount.get() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        server.stop(0);
        client.shutdown();

        assertTrue(hitCount.get() >= 1, "Expected at least one ingest request");
        String sentBody = bodies.poll();
        assertNotNull(sentBody);
        assertTrue(sentBody.contains("started"), "Expected 'started' status in payload");
        assertTrue(sentBody.contains("ProcessOrdersJob"), "Expected job name in payload");
    }

    @Test
    void trackSuccessAndFailureDoNotThrow() {
        // With an unreachable endpoint, errors should be swallowed silently
        TraceStaxClient client = new TraceStaxClient("ts_test_abc", "http://localhost:1", true, false);
        assertDoesNotThrow(() -> {
            client.trackStart("MyJob", "run-x", "default");
            client.trackSuccess("run-x", 100L);
        });
        assertDoesNotThrow(() -> {
            client.trackStart("MyJob", "run-y", "default");
            client.trackFailure("run-y", 50L, new IllegalStateException("test error"));
        });
        client.shutdown();
    }

    @Test
    void heartbeatDoesNotThrow() {
        TraceStaxClient client = new TraceStaxClient("ts_test_abc", "http://localhost:1", true, false);
        assertDoesNotThrow(() ->
            client.heartbeat("worker-1", new String[]{"default", "critical"}, 4)
        );
        client.shutdown();
    }

    @Test
    void snapshotDoesNotThrow() {
        TraceStaxClient client = new TraceStaxClient("ts_test_abc", "http://localhost:1", true, false);
        assertDoesNotThrow(() ->
            client.snapshot("default", 42, 3, 1)
        );
        client.shutdown();
    }

    // ── Integration test: events reach mock-ingest ───────────────────────

    @Test
    void integrationEventsReachMockIngest() throws Exception {
        if (!ingestAvailable()) {
            System.out.println("Skipping integration test — mock-ingest not available");
            return;
        }

        resetIngest();

        TraceStaxClient client = new TraceStaxClient("ts_test_abc", INGEST_URL, true, false);
        client.setFramework("quartz");

        client.trackStart("DailyReportJob", "job-integration-001", "scheduled");
        Thread.sleep(500);
        client.trackSuccess("job-integration-001", 1234L);
        Thread.sleep(1000);

        client.shutdown();

        String events = fetchEvents();
        assertTrue(events.contains("quartz"), "Expected quartz event in ingest: " + events);
        assertTrue(events.contains("DailyReportJob"), "Expected job name in ingest");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private boolean ingestAvailable() {
        try {
            URL url = new URI(INGEST_URL + "/test/health").toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            return conn.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private void resetIngest() throws Exception {
        URL url = new URI(INGEST_URL + "/test/reset").toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().close();
        conn.getResponseCode();
    }

    private String fetchEvents() throws Exception {
        URL url = new URI(INGEST_URL + "/test/events").toURL();
        try (InputStream is = url.openStream()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
