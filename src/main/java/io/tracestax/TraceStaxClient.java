package io.tracestax;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Core TraceStax client.
 *
 * <p>All HTTP calls are dispatched asynchronously on a single background thread.
 * Errors are logged at DEBUG level and never propagated to the caller, so the
 * SDK cannot destabilise the host application.
 *
 * <pre>{@code
 * TraceStaxClient client = new TraceStaxClient("ts_live_xxxx");
 *
 * String runId = UUID.randomUUID().toString();
 * client.trackStart(runId, "com.example.ReportJob", "reports");
 * // ... run the job ...
 * client.trackSuccess(runId, durationMs);
 *
 * // At application shutdown:
 * client.shutdown();
 * }</pre>
 */
public final class TraceStaxClient {

    private static final Logger LOG = LoggerFactory.getLogger(TraceStaxClient.class);

    private static final String DEFAULT_ENDPOINT = "https://ingest.tracestax.com";
    private static final String SDK_VERSION = "0.1.0";
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    /** Shared thread used for all fire-and-forget dispatches. */
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "tracestax-sender");
                t.setDaemon(true);
                t.setUncaughtExceptionHandler((th, ex) ->
                        LOG.warn("TraceStax sender thread terminated unexpectedly: {}", ex.getMessage(), ex));
                return t;
            });

    private static final int    CIRCUIT_OPEN_THRESHOLD = 3;
    private static final long   CIRCUIT_COOLDOWN_MS    = 30_000L;
    private static final long   MAX_FLUSH_INTERVAL_MS  = 60_000L;
    private static final int    MAX_QUEUE_SIZE         = 10_000;
    private static final int    TRIM_QUEUE_TO          = 5_000;

    private enum CircuitState { CLOSED, OPEN, HALF_OPEN }

    private final OkHttpClient http;
    private final ObjectMapper mapper;
    private final String apiKey;
    private final String endpoint;
    private final boolean enabled;
    private final boolean dryRun;
    /** Hostname resolved once at construction with a hard 1-second timeout to avoid DNS hangs. */
    private final String cachedHostname;
    private String framework  = "generic";
    private String workerKey  = null; // set by caller for thread dump identification

    // Resilience state
    private final AtomicInteger          consecutiveFailures = new AtomicInteger(0);
    private final AtomicReference<CircuitState> circuitState = new AtomicReference<>(CircuitState.CLOSED);
    private final AtomicLong             circuitOpenedAt     = new AtomicLong(0);
    private volatile long                pauseUntilMs        = 0;
    /** Grows exponentially on failure (up to MAX_FLUSH_INTERVAL_MS), halves on success. */
    private volatile long                dispatchDelayMs     = 0;

    // Queue depth guard — counts dispatches in-flight to bound memory usage.
    private final AtomicInteger          pendingCount        = new AtomicInteger(0);
    private final AtomicLong             droppedEvents       = new AtomicLong(0);

    /** Stores job identity (name, queue) from trackStart so success/failure events reuse it. */
    private final ConcurrentHashMap<String, String[]> pendingTasks = new ConcurrentHashMap<>();

    /**
     * Creates a client that sends events to the default TraceStax ingest endpoint.
     *
     * @param apiKey your TraceStax API key
     */
    public TraceStaxClient(final String apiKey) {
        this(apiKey, DEFAULT_ENDPOINT);
    }

    /**
     * Creates a client pointing at a custom endpoint with explicit enabled/dryRun flags.
     *
     * @param apiKey   your TraceStax API key
     * @param endpoint base URL, e.g. {@code https://ingest.tracestax.com}
     * @param enabled  whether the client is active ({@code null} to read from {@code TRACESTAX_ENABLED} env var)
     * @param dryRun   whether to log instead of sending ({@code null} to read from {@code TRACESTAX_DRY_RUN} env var)
     */
    public TraceStaxClient(final String apiKey, final String endpoint, final Boolean enabled, final Boolean dryRun) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("apiKey must not be blank");
        }
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint must not be blank");
        }
        this.apiKey = apiKey;
        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        this.enabled = enabled != null ? enabled : !"false".equals(System.getenv("TRACESTAX_ENABLED"));
        this.dryRun = dryRun != null ? dryRun : "true".equals(System.getenv("TRACESTAX_DRY_RUN"));
        this.http = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .writeTimeout(5, TimeUnit.SECONDS)
                .build();
        this.mapper = new ObjectMapper();
        this.mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.cachedHostname = resolveHostname();
    }

    /**
     * Creates a client pointing at a custom endpoint (useful for self-hosted or testing).
     *
     * @param apiKey   your TraceStax API key
     * @param endpoint base URL, e.g. {@code https://ingest.tracestax.com}
     */
    public TraceStaxClient(final String apiKey, final String endpoint) {
        this(apiKey, endpoint, null, null);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Records that a job run has started.
     *
     * @param runId    unique identifier for this execution (UUID recommended)
     * @param jobClass fully-qualified class name of the job
     * @param queue    name of the queue / topic the job was pulled from (may be {@code null})
     */
    public void trackStart(final String runId, final String jobClass, final String queue) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "task_event");
        payload.put("framework", framework);
        payload.put("language", "java");
        payload.put("sdk_version", SDK_VERSION);
        payload.put("status", "started");
        payload.put("worker", buildWorkerInfo());

        final String resolvedQueue = queue != null ? queue : "default";
        pendingTasks.put(runId, new String[]{jobClass, resolvedQueue});

        final Map<String, Object> task = new LinkedHashMap<>();
        task.put("name", jobClass);
        task.put("id", runId);
        task.put("queue", resolvedQueue);
        task.put("attempt", 1);
        payload.put("task", task);

        final Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("duration_ms", 0L);
        payload.put("metrics", metrics);

        sendAsync(endpoint + "/v1/ingest", payload);
    }

    /**
     * Records that a job run completed successfully.
     *
     * @param runId      the same ID passed to {@link #trackStart}
     * @param durationMs wall-clock duration of the run in milliseconds
     */
    public void trackSuccess(final String runId, final long durationMs) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "task_event");
        payload.put("framework", framework);
        payload.put("language", "java");
        payload.put("sdk_version", SDK_VERSION);
        payload.put("status", "succeeded");
        payload.put("worker", buildWorkerInfo());

        final String[] identity = pendingTasks.remove(runId);
        if (identity == null) {
            System.err.println("tracestax: WARNING trackSuccess called with unknown runId: " + runId + " — job name and queue will be reported as unknown/default");
        }
        final Map<String, Object> task = new LinkedHashMap<>();
        task.put("name",  identity != null ? identity[0] : "unknown");
        task.put("id",    runId);
        task.put("queue", identity != null ? identity[1] : "default");
        task.put("attempt", 1);
        payload.put("task", task);

        final Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("duration_ms", durationMs);
        payload.put("metrics", metrics);

        sendAsync(endpoint + "/v1/ingest", payload);
    }

    /**
     * Records that a job run failed.
     *
     * @param runId      the same ID passed to {@link #trackStart}
     * @param durationMs wall-clock duration of the run in milliseconds
     * @param throwable  the exception that caused the failure (may be {@code null})
     */
    public void trackFailure(final String runId, final long durationMs, final Throwable throwable) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("type", "task_event");
        payload.put("framework", framework);
        payload.put("language", "java");
        payload.put("sdk_version", SDK_VERSION);
        payload.put("status", "failed");
        payload.put("worker", buildWorkerInfo());

        final String[] identity = pendingTasks.remove(runId);
        if (identity == null) {
            System.err.println("tracestax: WARNING trackFailure called with unknown runId: " + runId + " — job name and queue will be reported as unknown/default");
        }
        final Map<String, Object> task = new LinkedHashMap<>();
        task.put("name",  identity != null ? identity[0] : "unknown");
        task.put("id",    runId);
        task.put("queue", identity != null ? identity[1] : "default");
        task.put("attempt", 1);
        payload.put("task", task);

        final Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("duration_ms", durationMs);
        payload.put("metrics", metrics);

        if (throwable != null) {
            final Map<String, String> error = new LinkedHashMap<>();
            error.put("type", throwable.getClass().getName());
            error.put("message", throwable.getMessage());
            payload.put("error", error);
        }
        sendAsync(endpoint + "/v1/ingest", payload);
    }

    /**
     * Sends a worker heartbeat.
     *
     * <p>The worker key is derived as {@code hostname:pid}. Framework is reported
     * as {@code "generic"} and language as {@code "java"}.
     *
     * @param workerKey   unique identifier for this worker (typically {@code hostname:pid})
     * @param queues      queue names this worker is consuming (may be {@code null})
     * @param concurrency number of concurrent worker threads
     */
    public void heartbeat(final String workerKey, final String[] queues, final int concurrency) {
        heartbeatWithHostname(workerKey, queues, concurrency, cachedHostname);
    }

    private void heartbeatWithHostname(
            final String workerKey,
            final String[] queues,
            final int concurrency,
            final String hostname) {
        final Map<String, Object> worker = new HashMap<>();
        worker.put("key", workerKey);
        worker.put("hostname", hostname);
        worker.put("pid", ProcessHandle.current().pid());
        worker.put("queues", queues != null ? java.util.Arrays.asList(queues) : Collections.emptyList());
        worker.put("concurrency", concurrency);

        final Map<String, Object> payload = new HashMap<>();
        payload.put("framework", framework);
        payload.put("language", "java");
        payload.put("sdk_version", SDK_VERSION);
        payload.put("timestamp", Instant.now().toString());
        payload.put("worker", worker);
        sendAsync(endpoint + "/v1/heartbeat", payload);
    }

    /**
     * Sends a queue-depth snapshot for a single queue.
     *
     * @param queueName   name of the queue being reported
     * @param depth       number of messages waiting in the queue
     * @param activeCount number of messages currently being processed (may be {@code null})
     * @param failedCount number of messages in the failed/dead-letter queue (may be {@code null})
     */
    public void snapshot(
            final String queueName,
            final int depth,
            final Integer activeCount,
            final Integer failedCount) {
        final Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("framework", framework);
        payload.put("worker_key", cachedHostname + ":" + ProcessHandle.current().pid());
        payload.put("timestamp", Instant.now().toString());

        final Map<String, Object> queueInfo = new LinkedHashMap<>();
        queueInfo.put("name", queueName);
        queueInfo.put("depth", depth);
        queueInfo.put("active", activeCount != null ? activeCount : 0);
        queueInfo.put("failed", failedCount != null ? failedCount : 0);
        queueInfo.put("throughput_per_min", 0);
        payload.put("queues", Collections.singletonList(queueInfo));

        sendAsync(endpoint + "/v1/snapshot", payload);
    }

    /**
     * Returns a snapshot of the client's internal health metrics.
     *
     * <p>Useful for monitoring SDK behaviour via application metrics exporters.
     */
    public ClientStats getStats() {
        return new ClientStats(
                pendingCount.get(),
                droppedEvents.get(),
                circuitState.get().name().toLowerCase(),
                consecutiveFailures.get()
        );
    }

    /**
     * Immutable snapshot of SDK health metrics.
     */
    public record ClientStats(int queueSize, long droppedEvents, String circuitState, int consecutiveFailures) {}

    /**
     * Gracefully shuts down the background executor.
     *
     * <p>Waits up to 5 seconds for any in-flight serialisation tasks to complete,
     * then cancels remaining work. Outstanding OkHttp calls dispatched before
     * shutdown was called may still complete asynchronously.
     */
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Sets the framework name reported in all payloads (default: {@code "generic"}).
     *
     * @param framework framework identifier, e.g. {@code "spring"}, {@code "quarkus"}
     * @return this client, for chaining
     */
    public TraceStaxClient setFramework(final String framework) {
        this.framework = framework != null ? framework : "generic";
        return this;
    }

    /**
     * Returns the currently configured framework name.
     */
    public String getFramework() {
        return framework;
    }

    /**
     * Sets the worker key used in thread dump payloads.
     */
    public TraceStaxClient setWorkerKey(final String workerKey) {
        this.workerKey = workerKey;
        return this;
    }

    /**
     * Pauses ingest delivery until the given epoch millisecond timestamp.
     * Called when the heartbeat response signals backpressure.
     */
    public void setPauseUntil(final long epochMs) {
        this.pauseUntilMs = epochMs;
    }

    /**
     * Sends a heartbeat synchronously and returns the directives map, or null on error.
     * The directives map has the shape: {@code {pause_ingest, pause_until_ms, commands}}.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> heartbeatSync(final String workerKey, final String[] queues, final int concurrency) {
        if (!enabled || dryRun) return null;
        final Map<String, Object> worker = new HashMap<>();
        worker.put("key", workerKey);
        worker.put("hostname", cachedHostname);
        worker.put("pid", ProcessHandle.current().pid());
        worker.put("queues", queues != null ? java.util.Arrays.asList(queues) : Collections.emptyList());
        worker.put("concurrency", concurrency);

        final Map<String, Object> payload = new HashMap<>();
        payload.put("framework", framework);
        payload.put("language", "java");
        payload.put("sdk_version", SDK_VERSION);
        payload.put("timestamp", Instant.now().toString());
        payload.put("worker", worker);

        try {
            final byte[] body = mapper.writeValueAsBytes(payload);
            final Request request = new Request.Builder()
                    .url(endpoint + "/v1/heartbeat")
                    .header("Authorization", "Bearer " + apiKey)
                    .header("Content-Type", "application/json")
                    .header("User-Agent", "tracestax-java/" + SDK_VERSION)
                    .post(RequestBody.create(body, JSON))
                    .build();
            try (Response resp = http.newCall(request).execute()) {
                final String retryAfter = resp.header("X-Retry-After");
                if (retryAfter != null) {
                    try {
                        final long secs = Long.parseLong(retryAfter.trim());
                        if (secs > 0) setPauseUntil(System.currentTimeMillis() + secs * 1_000L);
                    } catch (NumberFormatException ignored) {}
                }
                if (resp.code() == 401) {
                    // Auth failures are NOT counted as circuit-breaker failures — the
                    // circuit would open and silently drop all events, masking the real problem.
                    LOG.error("TraceStax auth failed (401) – check your API key.");
                    return null;
                }
                if (resp.isSuccessful() && resp.body() != null) {
                    recordSuccess();
                    // Cap response body at 1 MB to prevent OOM from large error pages.
                    final java.io.InputStream bodyStream = resp.body().byteStream();
                    final byte[] buf = new byte[1_048_576];
                    int totalRead = 0, bytesRead;
                    while (totalRead < buf.length &&
                           (bytesRead = bodyStream.read(buf, totalRead, buf.length - totalRead)) > 0) {
                        totalRead += bytesRead;
                    }
                    final String rawBody = new String(buf, 0, totalRead, java.nio.charset.StandardCharsets.UTF_8);
                    final Map<String, Object> respMap = mapper.readValue(rawBody, Map.class);
                    return (Map<String, Object>) respMap.get("directives");
                } else {
                    recordFailure();
                    return null;
                }
            }
        } catch (Exception e) {
            recordFailure();
            LOG.debug("TraceStax heartbeatSync failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Executes a server-issued command. Currently supports {@code "thread_dump"}.
     */
    public void executeCommand(final Map<String, Object> cmd) {
        if (!"thread_dump".equals(cmd.get("type"))) return;
        final String cmdId = (String) cmd.get("id");
        if (cmdId == null) return;

        final String wk = workerKey != null ? workerKey
                : (buildWorkerInfo().get("key") != null ? (String) buildWorkerInfo().get("key") : "java:unknown");
        final String dump = captureThreadDump();

        final Map<String, Object> dumpPayload = new LinkedHashMap<>();
        dumpPayload.put("cmd_id", cmdId);
        dumpPayload.put("worker_key", wk);
        dumpPayload.put("dump_text", dump);
        dumpPayload.put("language", "java");
        dumpPayload.put("sdk_version", SDK_VERSION);
        dumpPayload.put("captured_at", Instant.now().toString());

        sendAsync(endpoint + "/v1/dump", dumpPayload);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the local hostname with a hard 1-second timeout.
     *
     * <p>{@link java.net.InetAddress#getLocalHost()} performs a DNS reverse lookup that can
     * block for 30–120 seconds on misconfigured containers (common in Kubernetes). Running it
     * in a bounded {@link FutureTask} guarantees the calling thread is never stalled.
     */
    private static String resolveHostname() {
        final FutureTask<String> task = new FutureTask<>(
                () -> java.net.InetAddress.getLocalHost().getHostName());
        final Thread t = new Thread(task, "tracestax-hostname-resolver");
        t.setDaemon(true);
        t.start();
        try {
            return task.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.debug("TraceStax could not resolve hostname within 1s, using 'unknown': {}", e.getMessage());
            return "unknown";
        }
    }

    private Map<String, Object> buildWorkerInfo() {
        final Map<String, Object> worker = new HashMap<>();
        worker.put("key", cachedHostname + ":" + ProcessHandle.current().pid());
        worker.put("hostname", cachedHostname);
        worker.put("pid", ProcessHandle.current().pid());
        worker.put("queues", Collections.emptyList());
        worker.put("concurrency", 1);
        return worker;
    }

    /**
     * Serialises {@code payload} on the background executor then enqueues an
     * async OkHttp call.  Any exception is caught and logged; it is never
     * re-thrown.
     */
    private void sendAsync(final String url, final Map<String, Object> payload) {
        if (!enabled) {
            return;
        }
        if (dryRun) {
            LOG.info("[tracestax dry-run] {} {}", url, payload);
            return;
        }
        if (executor.isShutdown()) {
            LOG.debug("TraceStaxClient is shut down; dropping event for {}", url);
            return;
        }
        // Circuit breaker check
        if (!circuitAllow()) {
            return;
        }
        // Backpressure pause
        if (pauseUntilMs > 0 && System.currentTimeMillis() < pauseUntilMs) {
            return;
        }
        // Queue depth guard — prevent unbounded memory growth when server is slow/down
        if (pendingCount.get() >= MAX_QUEUE_SIZE) {
            droppedEvents.incrementAndGet();
            LOG.warn("TraceStax event queue full ({} pending), dropping event", pendingCount.get());
            return;
        }
        pendingCount.incrementAndGet();
        final long delay = dispatchDelayMs;
        final Runnable task = () -> {
            try {
                final byte[] body = mapper.writeValueAsBytes(payload);
                // Guard against huge payloads that would waste bandwidth and
                // could OOM the host. 512 KB matches every other SDK's limit.
                if (body.length > 512 * 1024) {
                    pendingCount.decrementAndGet();
                    LOG.warn("TraceStax payload exceeds 512 KB ({} bytes), dropping", body.length);
                    return;
                }
                final Request request = new Request.Builder()
                        .url(url)
                        .header("Authorization", "Bearer " + apiKey)
                        .header("X-Api-Key", apiKey)
                        .header("Content-Type", "application/json")
                        .header("User-Agent", "tracestax-java/" + SDK_VERSION)
                        .post(RequestBody.create(body, JSON))
                        .build();

                http.newCall(request).enqueue(new Callback() {
                    @Override
                    public void onFailure(final Call call, final IOException e) {
                        pendingCount.decrementAndGet();
                        recordFailure();
                        LOG.debug("TraceStax send failed [{}]: {}", url, e.getMessage());
                    }

                    @Override
                    public void onResponse(final Call call, final Response response) {
                        pendingCount.decrementAndGet();
                        try {
                            // Honor server-driven backpressure pause
                            final String retryAfter = response.header("X-Retry-After");
                            if (retryAfter != null) {
                                try {
                                    final long secs = Long.parseLong(retryAfter.trim());
                                    if (secs > 0) setPauseUntil(System.currentTimeMillis() + secs * 1_000L);
                                } catch (NumberFormatException ignored) {}
                            }
                            if (response.code() == 401) {
                                // Auth failures are NOT counted as circuit-breaker failures — the
                                // circuit would open and silently drop all events, masking the real problem.
                                LOG.error("TraceStax auth failed (401) – check your API key.");
                            } else if (!response.isSuccessful()) {
                                recordFailure();
                                LOG.debug("TraceStax non-2xx response [{}]: {}", url, response.code());
                            } else {
                                recordSuccess();
                            }
                        } finally {
                            response.close();
                        }
                    }
                });
            } catch (Exception e) {
                pendingCount.decrementAndGet();
                recordFailure();
                LOG.debug("TraceStax payload serialisation failed [{}]: {}", url, e.getMessage());
            }
        };
        if (delay > 0) {
            executor.schedule(task, delay, TimeUnit.MILLISECONDS);
        } else {
            executor.submit(task);
        }
    }

    private boolean circuitAllow() {
        final CircuitState state = circuitState.get();
        if (state == CircuitState.OPEN) {
            final long elapsed = Math.max(0L, System.currentTimeMillis() - circuitOpenedAt.get());
            if (elapsed < CIRCUIT_COOLDOWN_MS) return false;
            circuitState.compareAndSet(CircuitState.OPEN, CircuitState.HALF_OPEN);
        }
        return true;
    }

    private void recordSuccess() {
        consecutiveFailures.set(0);
        circuitState.set(CircuitState.CLOSED);
        dispatchDelayMs = Math.max(0L, dispatchDelayMs / 2);
    }

    private void recordFailure() {
        final int failures = consecutiveFailures.incrementAndGet();
        dispatchDelayMs = Math.min(MAX_FLUSH_INTERVAL_MS, Math.max(500L, dispatchDelayMs * 2));
        if (failures >= CIRCUIT_OPEN_THRESHOLD && circuitState.compareAndSet(CircuitState.CLOSED, CircuitState.OPEN)) {
            circuitOpenedAt.set(System.currentTimeMillis());
            LOG.warn("TraceStax unreachable, circuit open, events dropped");
        } else if (circuitState.compareAndSet(CircuitState.HALF_OPEN, CircuitState.OPEN)) {
            circuitOpenedAt.set(System.currentTimeMillis());
        }
    }

    private static String captureThreadDump() {
        final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threads = bean.dumpAllThreads(true, true);
        final StringBuilder sb = new StringBuilder("=== TraceStax Java Thread Dump ===\n");
        sb.append("PID: ").append(ProcessHandle.current().pid()).append("\n");
        sb.append("Timestamp: ").append(Instant.now()).append("\n\n");
        for (final ThreadInfo ti : threads) {
            sb.append(ti.toString());
        }
        final String result = sb.toString();
        return result.length() > 500_000 ? result.substring(0, 500_000) : result;
    }
}
