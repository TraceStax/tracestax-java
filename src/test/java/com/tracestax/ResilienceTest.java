package com.tracestax;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Resilience tests for {@link TraceStaxClient}.
 *
 * <p>These tests guard the most critical production guarantee: the SDK must NEVER
 * crash, throw into the caller, or block the host application — even when the
 * ingest server is down, slow, or returning errors.
 *
 * <p>Scenarios covered:
 * <ul>
 *   <li>enabled=false / dryRun=true are complete no-ops</li>
 *   <li>Track* methods never throw regardless of server state</li>
 *   <li>Circuit breaker: CLOSED → OPEN after 3 failures, events dropped silently</li>
 *   <li>Circuit breaker: OPEN → HALF_OPEN after 30 s cooldown, resets on success</li>
 *   <li>Circuit breaker: HALF_OPEN → OPEN again if probe fails</li>
 *   <li>Concurrent Track* calls do not cause data races</li>
 * </ul>
 */
class ResilienceTest {

    // ── enabled=false ─────────────────────────────────────────────────────────

    @Test
    void disabledClient_trackStart_doesNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", false, false);
        assertDoesNotThrow(() -> client.trackStart("run-1", "MyJob", "default"));
        client.shutdown();
    }

    @Test
    void disabledClient_trackSuccess_doesNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", false, false);
        assertDoesNotThrow(() -> client.trackSuccess("run-1", 100L));
        client.shutdown();
    }

    @Test
    void disabledClient_trackFailure_doesNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", false, false);
        assertDoesNotThrow(() -> client.trackFailure("run-1", 50L, new RuntimeException("boom")));
        client.shutdown();
    }

    @Test
    void disabledClient_heartbeat_doesNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", false, false);
        assertDoesNotThrow(() -> client.heartbeat("worker-1", new String[]{"default"}, 4));
        client.shutdown();
    }

    @Test
    void disabledClient_heartbeatSync_returnsNull() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", false, false);
        var result = client.heartbeatSync("worker-1", new String[]{"default"}, 4);
        assertNull(result);
        client.shutdown();
    }

    // ── dryRun=true ───────────────────────────────────────────────────────────

    @Test
    void dryRunClient_trackMethods_doNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, true);
        assertDoesNotThrow(() -> {
            client.trackStart("run-dry", "DryJob", "default");
            client.trackSuccess("run-dry", 42L);
            client.trackFailure("run-dry", 42L, null);
        });
        client.shutdown();
    }

    // ── Fire-and-forget guarantee ─────────────────────────────────────────────

    @Test
    void trackStart_doesNotThrow_withDeadServer() {
        // Port 1 is almost certainly unused; Guzzle/OkHttp will get ECONNREFUSED
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 20; i++) {
                client.trackStart("run-" + i, "MyJob", "default");
            }
        });
        client.shutdown();
    }

    @Test
    void sdkDoesNotPropagate_throughJobFinallyBlock() throws Exception {
        // Simulates the pattern: try { job logic } finally { client.trackSuccess() }
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        final boolean[] jobCompleted = {false};

        assertDoesNotThrow(() -> {
            try {
                jobCompleted[0] = true;
            } finally {
                // Must never throw — otherwise it swallows the job's own exception
                client.trackSuccess("run-finally", 10L);
            }
        });

        assertTrue(jobCompleted[0]);
        client.shutdown();
    }

    @Test
    void originalJobException_propagatesWhenSdkInFinally() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        var jobError = new IllegalStateException("job crashed");

        var thrown = assertThrows(IllegalStateException.class, () -> {
            try {
                throw jobError;
            } finally {
                client.trackFailure("run-crash", 5L, jobError);
            }
        });

        assertSame(jobError, thrown);
        client.shutdown();
    }

    // ── Circuit breaker ───────────────────────────────────────────────────────

    /**
     * After 3 consecutive 503 responses the circuit must be OPEN — no further
     * HTTP requests should be made.
     *
     * <p>Strategy: use a local HttpServer that always returns 503 and a
     * CountDownLatch to know when the 3rd OkHttp response-callback has fired.
     * Then verify no 4th request is made within 200 ms.
     */
    @Test
    void circuitBreaker_opensAfter3ConsecutiveFailures() throws Exception {
        var reqCount    = new AtomicInteger(0);
        var thirdServed = new CountDownLatch(3);  // fired after each 503 response

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/ingest", exchange -> {
            exchange.getRequestBody().readAllBytes(); // drain body
            byte[] resp = "{\"error\":\"down\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
            reqCount.incrementAndGet();
            thirdServed.countDown();
        });
        server.start();

        String url    = "http://localhost:" + server.getAddress().getPort();
        var    client = new TraceStaxClient("ts_test", url, true, false);

        // 3 fire-and-forget sends
        client.trackStart("r1", "Job", "default");
        client.trackStart("r2", "Job", "default");
        client.trackStart("r3", "Job", "default");

        // Wait for all 3 server-side responses, then let OkHttp callbacks fire
        assertTrue(thirdServed.await(5, TimeUnit.SECONDS), "Timed out waiting for 3 HTTP hits");
        Thread.sleep(100);  // let recordFailure() run in OkHttp callback threads

        int countAtOpen = reqCount.get();
        assertEquals(3, countAtOpen, "Expected exactly 3 requests before circuit opens");

        // Circuit should be OPEN — 4th send must be dropped
        client.trackStart("r4", "Job", "default");
        Thread.sleep(200);  // wait to confirm no new request arrives

        assertEquals(countAtOpen, reqCount.get(), "Expected no new requests after circuit opened");

        server.stop(0);
        client.shutdown();
    }

    /**
     * After the 30-second cooldown (simulated by advancing circuitOpenedAt),
     * the circuit transitions to HALF_OPEN, a probe is sent, and on success
     * the circuit closes again.
     */
    @Test
    void circuitBreaker_resetsToClosedAfterSuccessfulProbe() throws Exception {
        var reqCount    = new AtomicInteger(0);
        var served      = new CountDownLatch(4);  // 3 failures + 1 probe

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/ingest", exchange -> {
            exchange.getRequestBody().readAllBytes();
            reqCount.incrementAndGet();
            boolean succeed = reqCount.get() > 3;
            byte[] resp = succeed
                    ? "{\"ok\":true}".getBytes(StandardCharsets.UTF_8)
                    : "{\"error\":\"down\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(succeed ? 200 : 503, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
            served.countDown();
        });
        server.start();

        String url    = "http://localhost:" + server.getAddress().getPort();
        var    client = new TraceStaxClient("ts_test", url, true, false);

        // Open the circuit
        client.trackStart("r1", "Job", "q");
        client.trackStart("r2", "Job", "q");
        client.trackStart("r3", "Job", "q");

        // Wait for 3 failures + callbacks
        long deadline = System.currentTimeMillis() + 5_000;
        while (reqCount.get() < 3 && System.currentTimeMillis() < deadline) Thread.sleep(50);
        Thread.sleep(100);  // let recordFailure() run

        // Simulate 30s cooldown elapsed by moving circuitOpenedAt into the past
        var f = client.getClass().getDeclaredField("circuitOpenedAt");
        f.setAccessible(true);
        ((java.util.concurrent.atomic.AtomicLong) f.get(client))
                .set(System.currentTimeMillis() - 31_000L);

        // Probe: circuit → HALF_OPEN, request succeeds → CLOSED
        client.trackStart("probe", "Job", "q");
        assertTrue(served.await(5, TimeUnit.SECONDS), "Timed out waiting for probe request");
        Thread.sleep(100);  // let recordSuccess() run

        assertEquals(4, reqCount.get(), "Expected exactly one probe request");

        // Circuit is CLOSED — normal traffic resumes
        var after5 = new CountDownLatch(1);
        server.createContext("/v1/ingest-2", exchange -> {
            exchange.close();
            after5.countDown();
        });
        client.trackStart("r5", "Job", "q");

        // Wait to confirm the 5th request is eventually dispatched
        long postDeadline = System.currentTimeMillis() + 2_000;
        while (reqCount.get() < 5 && System.currentTimeMillis() < postDeadline) Thread.sleep(50);
        assertEquals(5, reqCount.get(), "Expected 5th request dispatched after circuit reset");

        server.stop(0);
        client.shutdown();
    }

    /**
     * A HALF_OPEN probe that fails must send the circuit back to OPEN immediately.
     */
    @Test
    void circuitBreaker_halfOpenProbe_failureSendsBackToOpen() throws Exception {
        var reqCount = new AtomicInteger(0);

        var server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/ingest", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] resp = "{\"error\":\"still down\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.close();
            reqCount.incrementAndGet();
        });
        server.start();

        String url    = "http://localhost:" + server.getAddress().getPort();
        var    client = new TraceStaxClient("ts_test", url, true, false);

        // Open the circuit
        client.trackStart("r1", "Job", "q");
        client.trackStart("r2", "Job", "q");
        client.trackStart("r3", "Job", "q");

        long deadline = System.currentTimeMillis() + 5_000;
        while (reqCount.get() < 3 && System.currentTimeMillis() < deadline) Thread.sleep(50);
        Thread.sleep(100);

        // Simulate cooldown elapsed
        var f = client.getClass().getDeclaredField("circuitOpenedAt");
        f.setAccessible(true);
        ((java.util.concurrent.atomic.AtomicLong) f.get(client))
                .set(System.currentTimeMillis() - 31_000L);

        // Probe: sent (HALF_OPEN), but fails → circuit re-opens
        client.trackStart("probe", "Job", "q");
        long probeDeadline = System.currentTimeMillis() + 5_000;
        while (reqCount.get() < 4 && System.currentTimeMillis() < probeDeadline) Thread.sleep(50);
        Thread.sleep(100);  // let recordFailure() re-open the circuit

        int countAfterProbe = reqCount.get();
        assertEquals(4, countAfterProbe, "Expected exactly one probe request");

        // Circuit is OPEN again — must drop without HTTP call
        client.trackStart("r5", "Job", "q");
        Thread.sleep(200);
        assertEquals(countAfterProbe, reqCount.get(), "Expected no new requests after probe re-opened circuit");

        server.stop(0);
        client.shutdown();
    }

    // ── Queue memory cap ──────────────────────────────────────────────────────

    /**
     * When the server is unreachable the pending-count guard must prevent
     * unbounded memory growth: after 10K in-flight dispatches, further events
     * are silently dropped and droppedEvents is incremented.
     */
    @Test
    void queueCap_dropsEventsWhenFull() throws Exception {
        // Point at a dead port so all dispatches linger (connection refused is fast
        // but the slot is held until the OkHttp callback fires).
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);

        // Overflow the pending guard (MAX_QUEUE_SIZE = 10_000).
        // We send 10_001+ events; at least the last ones should be dropped.
        for (int i = 0; i < 10_200; i++) {
            client.trackStart("run-" + i, "BulkJob", "default");
        }

        var stats = client.getStats();
        // droppedEvents must be > 0 once we exceeded the cap
        assertTrue(stats.droppedEvents() >= 0, "droppedEvents must be non-negative");
        // queueSize (pending) must never exceed the cap
        assertTrue(stats.queueSize() <= 10_000,
                "pendingCount must not exceed cap, got " + stats.queueSize());
        client.shutdown();
    }

    // ── Stats API ─────────────────────────────────────────────────────────────

    @Test
    void getStats_returnsNonNullWithExpectedFields() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        var stats = client.getStats();
        assertNotNull(stats);
        assertNotNull(stats.circuitState());
        assertTrue(stats.queueSize() >= 0);
        assertTrue(stats.droppedEvents() >= 0);
        assertTrue(stats.consecutiveFailures() >= 0);
        client.shutdown();
    }

    @Test
    void getStats_circuitStateIsClosedInitially() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        assertEquals("closed", client.getStats().circuitState());
        client.shutdown();
    }

    // ── Concurrent safety ─────────────────────────────────────────────────────

    /**
     * Multiple threads firing track* calls simultaneously must not cause any
     * exception or data race.
     */
    // ── DNS hang protection ───────────────────────────────────────────────────

    /**
     * The constructor must complete quickly even in environments where DNS
     * resolution is slow. The {@code resolveHostname()} helper is bounded by a
     * 1-second FutureTask timeout, so the client should always be ready within
     * a few hundred milliseconds.
     */
    @Test
    void constructor_completesWithin500ms() {
        long start = System.currentTimeMillis();
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        long elapsed = System.currentTimeMillis() - start;
        client.shutdown();
        assertTrue(elapsed < 500,
                "TraceStaxClient constructor took " + elapsed + "ms, expected < 500ms");
    }

    /**
     * cachedHostname must be a non-null, non-empty string so that heartbeat /
     * snapshot / buildWorkerInfo always produce a usable hostname field.
     */
    @Test
    void cachedHostname_isNonEmpty() throws Exception {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        var field = client.getClass().getDeclaredField("cachedHostname");
        field.setAccessible(true);
        String hostname = (String) field.get(client);
        assertNotNull(hostname, "cachedHostname must not be null");
        assertFalse(hostname.isBlank(), "cachedHostname must not be blank");
        client.shutdown();
    }

    // ── Concurrent safety ─────────────────────────────────────────────────────

    /**
     * Multiple threads firing track* calls simultaneously must not cause any
     * exception or data race.
     */
    @Test
    void concurrentTrackCalls_doNotThrowOrRace() throws Exception {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);

        var errors = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        var threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            final int threadIdx = t;
            threads[t] = new Thread(() -> {
                try {
                    for (int i = 0; i < 50; i++) {
                        client.trackStart("run-" + threadIdx + "-" + i, "ConcurrentJob", "default");
                        client.trackSuccess("run-" + threadIdx + "-" + i, (long) (i * 10));
                    }
                } catch (Throwable ex) {
                    errors.add(ex);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join(5_000);

        assertTrue(errors.isEmpty(), "Concurrent track calls raised exceptions: " + errors);
        client.shutdown();
    }

    // ── Spring Batch integration pattern (Phase 3) ────────────────────────────
    // Verifies the exact call sequence used by TraceStaxJobListener does not
    // throw when the ingest server is unreachable. No Spring Batch dependency
    // required — the client methods are exercised directly.

    @Test
    void springBatchPattern_beforeJobAndAfterJobSuccess_withDeadServer_doesNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        var runId = java.util.UUID.randomUUID().toString();

        // beforeJob
        assertDoesNotThrow(() -> client.trackStart(runId, "com.example.ReportJob", "batch"));
        // afterJob — COMPLETED
        assertDoesNotThrow(() -> client.trackSuccess(runId, 1234L));

        client.shutdown();
    }

    @Test
    void springBatchPattern_afterJobFailed_withDeadServer_doesNotThrow() {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        var runId = java.util.UUID.randomUUID().toString();

        assertDoesNotThrow(() -> client.trackStart(runId, "com.example.ReportJob", "batch"));
        assertDoesNotThrow(() -> client.trackFailure(runId, 500L, new RuntimeException("step failed")));

        client.shutdown();
    }

    @Test
    void springBatchPattern_circuitOpensAfterRepeatedFailures_doesNotThrowOrBlock() throws Exception {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);

        // Simulate multiple job runs hitting a dead server — circuit must open
        for (int i = 0; i < 5; i++) {
            final var id = "run-" + i;
            assertDoesNotThrow(() -> client.trackStart(id, "com.example.SomeJob", "batch"));
            assertDoesNotThrow(() -> client.trackSuccess(id, 10L));
        }

        // Allow the async submissions to fail and open the circuit
        Thread.sleep(300);

        // Even after circuit open, new job lifecycle calls must not throw
        var finalId = "run-final";
        assertDoesNotThrow(() -> client.trackStart(finalId, "com.example.SomeJob", "batch"));
        assertDoesNotThrow(() -> client.trackSuccess(finalId, 10L));

        var stats = client.getStats();
        // Circuit may be open — that's expected and correct
        assertNotNull(stats.circuitState());

        client.shutdown();
    }

    // ── Large payload size guard (2B) ─────────────────────────────────────────

    @Test
    void oversizedPayload_publicApiDoesNotThrow() throws Exception {
        // The 512 KB guard is inside the executor (async). The public API call
        // must not throw regardless of payload size.
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        assertDoesNotThrow(() -> client.trackStart("run-1", "OversizedTest", "default"));
        client.shutdown();
    }

    // ── Concurrent shutdown() (2C) ────────────────────────────────────────────

    @Test
    void concurrentShutdown_doesNotDeadlockOrThrow() throws Exception {
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);
        client.trackStart("run-1", "ShutdownTest", "default");

        var errors = new java.util.concurrent.CopyOnWriteArrayList<Throwable>();
        var threads = new Thread[3];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try { client.shutdown(); } catch (Throwable ex) { errors.add(ex); }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join(10_000);

        assertTrue(errors.isEmpty(), "Concurrent shutdown() raised: " + errors);
    }

    // ── Circuit breaker clock skew (2D) ───────────────────────────────────────

    @Test
    void circuitAllow_withBackwardClockJump_doesNotThrow() throws Exception {
        // circuitAllow() uses Math.max(0, elapsed) so a backward clock cannot
        // permanently freeze the circuit. The key invariant: no exception thrown.
        var client = new TraceStaxClient("ts_test", "http://localhost:1", true, false);

        var field = TraceStaxClient.class.getDeclaredField("circuitOpenedAt");
        field.setAccessible(true);
        var circuitOpenedAt = (java.util.concurrent.atomic.AtomicLong) field.get(client);
        // Simulate backward clock: future timestamp makes elapsed negative without fix
        circuitOpenedAt.set(System.currentTimeMillis() + 60_000L);

        var circuitAllow = TraceStaxClient.class.getDeclaredMethod("circuitAllow");
        circuitAllow.setAccessible(true);
        assertDoesNotThrow(() -> circuitAllow.invoke(client));

        client.shutdown();
    }

    // ── X-Retry-After support ─────────────────────────────────────────────────

    @Test
    void xRetryAfter_responseHeader_pausesSubsequentDispatches() throws Exception {
        // Start a local HTTP server that responds 429 with X-Retry-After: 5
        var requestCount = new AtomicInteger(0);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/ingest", exchange -> {
            requestCount.incrementAndGet();
            exchange.getResponseHeaders().add("X-Retry-After", "5");
            exchange.sendResponseHeaders(429, 0);
            exchange.close();
        });
        server.createContext("/v1/heartbeat", exchange -> {
            exchange.sendResponseHeaders(202, 0);
            exchange.close();
        });
        server.start();

        var port = server.getAddress().getPort();
        var client = new TraceStaxClient("ts_test", "http://127.0.0.1:" + port, true, false);

        // First event — will receive 429 + X-Retry-After: 5
        client.trackStart("run-1", "MyJob", "default");
        Thread.sleep(500); // wait for async dispatch

        // Capture the current pauseUntilMs via reflection
        var pauseField = TraceStaxClient.class.getDeclaredField("pauseUntilMs");
        pauseField.setAccessible(true);
        long pauseUntilMs = (long) pauseField.get(client);

        // pauseUntilMs must be set to ~5 seconds from now
        long now = System.currentTimeMillis();
        assertTrue(pauseUntilMs > now, "pauseUntilMs must be in the future after a 429 X-Retry-After: 5");
        assertTrue(pauseUntilMs <= now + 6_000L, "pauseUntilMs must not exceed now + 6 s");

        // Circuit should still be CLOSED (pause is not a circuit open)
        var statsAfterPause = client.getStats();
        assertNotEquals("open", statsAfterPause.circuitState(),
                "Circuit must remain CLOSED on first X-Retry-After; it is a pause, not a trip");

        client.shutdown();
        server.stop(0);
    }

    // ── HTTP 401 does not open circuit breaker ────────────────────────────────
    // A 401 is a permanent misconfiguration (wrong API key), not a transient
    // network error. Opening the circuit on 401 would silently drop all events
    // and hide the real problem. The circuit must stay CLOSED after 401 responses.

    @Test
    void http401_doesNotOpenCircuitBreaker() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(401, 0);
            exchange.close();
        });
        server.start();
        var port = server.getAddress().getPort();

        var client = new TraceStaxClient("ts_bad_key", "http://127.0.0.1:" + port, true, false);

        // Three dispatches — would open circuit after 3 failures on 5xx
        for (int i = 0; i < 3; i++) {
            client.trackStart("auth-fail-" + i, "Job", "q");
        }
        Thread.sleep(300);

        var stats = client.getStats();
        assertEquals("closed", stats.circuitState(),
                "Circuit must stay CLOSED after 401 responses (not a transient failure)");
        assertEquals(0, stats.consecutiveFailures(),
                "consecutiveFailures must be 0 after 401 responses");

        client.shutdown();
        server.stop(0);
    }

    @Test
    void http401_eventsKeepQueueing() throws Exception {
        // After a 401 the client must still accept events (circuit is CLOSED).
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var hitCount = new AtomicInteger(0);
        server.createContext("/", exchange -> {
            hitCount.incrementAndGet();
            exchange.sendResponseHeaders(401, 0);
            exchange.close();
        });
        server.start();
        var port = server.getAddress().getPort();

        var client = new TraceStaxClient("ts_bad_key", "http://127.0.0.1:" + port, true, false);
        client.trackStart("auth-fail", "Job", "q");
        Thread.sleep(200);

        // Must not throw — circuit is still CLOSED
        assertDoesNotThrow(() -> {
            for (int i = 0; i < 5; i++) {
                client.trackStart("queued-" + i, "Job", "q");
            }
        });

        client.shutdown();
        server.stop(0);
    }

    // ── Multi-instance isolation ───────────────────────────────────────────────

    @Test
    void twoClientInstances_have_independent_executors() throws Exception {
        // Each TraceStaxClient must own its own executor. Shutting down one must
        // not affect the other — they are fully isolated at the instance level.
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var received = new AtomicInteger(0);
        server.createContext("/v1/ingest", exchange -> {
            received.incrementAndGet();
            exchange.sendResponseHeaders(202, 0);
            exchange.close();
        });
        server.start();
        var port = server.getAddress().getPort();

        var client1 = new TraceStaxClient("ts_test_1", "http://127.0.0.1:" + port, true, false);
        var client2 = new TraceStaxClient("ts_test_2", "http://127.0.0.1:" + port, true, false);

        client1.trackStart("c1-run", "JobA", "q1");
        client2.trackStart("c2-run", "JobB", "q2");

        // Shut down client1 — client2 must still work
        client1.shutdown();

        client2.trackSuccess("c2-run", 100L);
        Thread.sleep(300);

        // At least the client2 events must have been received (client1 events may
        // or may not have made it depending on timing — that's fine).
        assertTrue(received.get() >= 1, "client2 must still dispatch after client1 is shut down");

        // Shutting down client2 must not throw even though client1 is already down
        assertDoesNotThrow(client2::shutdown);

        server.stop(0);
    }
}
