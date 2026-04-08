package io.tracestax.quartz;

import io.tracestax.TraceStaxClient;
import org.junit.jupiter.api.*;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link TraceStaxJobListener} with a real Quartz scheduler.
 * These tests run only when {@code TRACESTAX_INGEST_URL} is set (i.e. in docker-compose.test).
 */
class QuartzIntegrationTest {

    private static final String INGEST_URL =
        System.getenv().getOrDefault("TRACESTAX_INGEST_URL", "http://localhost:4001");

    @BeforeEach
    void resetIngest() throws Exception {
        if (!ingestAvailable()) return;
        URL url = new URI(INGEST_URL + "/test/reset").toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().close();
        conn.getResponseCode();
    }

    /** A trivial Quartz job used purely for testing. */
    public static class HelloJob implements Job {
        @Override
        public void execute(JobExecutionContext context) {
            // no-op — the listener captures start/success around this
        }
    }

    /** A Quartz job that throws to simulate failure. */
    public static class FailingJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            throw new JobExecutionException(new RuntimeException("simulated failure"));
        }
    }

    @Test
    void listenerTracksSuccessfulJobExecution() throws Exception {
        if (!ingestAvailable()) {
            System.out.println("Skipping Quartz integration test — mock-ingest not available");
            return;
        }

        TraceStaxClient client = new TraceStaxClient("ts_test_abc", INGEST_URL, true, false);
        client.setFramework("quartz");

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        scheduler.getListenerManager().addJobListener(new TraceStaxJobListener(client));
        scheduler.start();

        JobDetail job = JobBuilder.newJob(HelloJob.class)
            .withIdentity("helloJob", "test")
            .build();

        Trigger trigger = TriggerBuilder.newTrigger()
            .withIdentity("helloTrigger", "test")
            .startNow()
            .build();

        scheduler.scheduleJob(job, trigger);

        // Allow the job to execute and events to flush
        Thread.sleep(2000);
        scheduler.shutdown(true);
        client.shutdown();

        String events = fetchEvents();
        assertTrue(events.contains("quartz"), "Expected quartz in events: " + events);
        assertTrue(events.contains("HelloJob"), "Expected HelloJob class name in events: " + events);
    }

    @Test
    void listenerTracksFailedJobExecution() throws Exception {
        if (!ingestAvailable()) {
            System.out.println("Skipping Quartz integration test — mock-ingest not available");
            return;
        }

        resetIngest();

        TraceStaxClient client = new TraceStaxClient("ts_test_abc", INGEST_URL, true, false);
        client.setFramework("quartz");

        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.getListenerManager().addJobListener(new TraceStaxJobListener(client));
        scheduler.start();

        JobDetail job = JobBuilder.newJob(FailingJob.class)
            .withIdentity("failingJob", "test")
            .build();

        Trigger trigger = TriggerBuilder.newTrigger()
            .withIdentity("failingTrigger", "test")
            .startNow()
            .build();

        scheduler.scheduleJob(job, trigger);

        Thread.sleep(2000);
        scheduler.shutdown(true);
        client.shutdown();

        String events = fetchEvents();
        assertTrue(events.contains("failed") || events.contains("FailingJob"),
            "Expected failed event for FailingJob: " + events);
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

    private String fetchEvents() throws Exception {
        URL url = new URI(INGEST_URL + "/test/events").toURL();
        try (InputStream is = url.openStream()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
