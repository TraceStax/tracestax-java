package io.tracestax.quartz;

import io.tracestax.TraceStaxClient;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Quartz {@link JobListener} that automatically tracks job lifecycle events with TraceStax.
 *
 * <p>Register it once when configuring your scheduler:
 *
 * <pre>{@code
 * TraceStaxClient client = new TraceStaxClient("ts_live_xxxx");
 * scheduler.getListenerManager().addJobListener(new TraceStaxJobListener(client));
 * }</pre>
 *
 * <p>The listener is global by default (no matchers), so it tracks every job in
 * the scheduler.  To limit coverage, pass {@link org.quartz.impl.matchers.GroupMatcher}
 * or other matchers to {@code addJobListener}.
 */
public final class TraceStaxJobListener implements JobListener {

    private static final Logger LOG = LoggerFactory.getLogger(TraceStaxJobListener.class);
    private static final String LISTENER_NAME = "TraceStaxJobListener";

    /** Key: Quartz JobKey string → Value: [runId, startTimeMs]. */
    private final ConcurrentHashMap<String, long[]> runState = new ConcurrentHashMap<>();
    /** Key: Quartz JobKey string → Value: run UUID. */
    private final ConcurrentHashMap<String, String> runIds = new ConcurrentHashMap<>();

    private final TraceStaxClient client;

    /**
     * @param client a configured {@link TraceStaxClient}
     */
    public TraceStaxJobListener(final TraceStaxClient client) {
        if (client == null) {
            throw new IllegalArgumentException("client must not be null");
        }
        this.client = client;
    }

    @Override
    public String getName() {
        return LISTENER_NAME;
    }

    /**
     * Called by Quartz immediately before the job's {@code execute} method.
     * Sends a {@code task.started} event to TraceStax.
     */
    @Override
    public void jobToBeExecuted(final JobExecutionContext context) {
        final String key = jobKeyString(context);
        final String runId = UUID.randomUUID().toString();

        runIds.put(key, runId);
        runState.put(key, new long[]{System.currentTimeMillis()});

        final String jobClass = context.getJobDetail().getJobClass().getName();
        final String queue = context.getJobDetail().getKey().getGroup();

        try {
            client.trackStart(runId, jobClass, queue);
        } catch (Exception e) {
            // TraceStaxClient already swallows errors internally; belt-and-suspenders.
            LOG.debug("TraceStaxJobListener.jobToBeExecuted: unexpected exception", e);
        }
    }

    /**
     * Called by Quartz when the job is vetoed by a {@link org.quartz.TriggerListener}.
     * Cleans up in-flight state without sending any event.
     */
    @Override
    public void jobExecutionVetoed(final JobExecutionContext context) {
        final String key = jobKeyString(context);
        runIds.remove(key);
        runState.remove(key);
    }

    /**
     * Called by Quartz after the job's {@code execute} method returns (or throws).
     * Sends either a {@code task.succeeded} or {@code task.failed} event.
     */
    @Override
    public void jobWasExecuted(final JobExecutionContext context, final JobExecutionException jobException) {
        final String key = jobKeyString(context);

        final String runId = runIds.remove(key);
        final long[] state = runState.remove(key);

        if (runId == null || state == null) {
            // Defensive: started event was never recorded (e.g. listener added mid-flight)
            LOG.debug("TraceStaxJobListener: no start record found for job key {}", key);
            return;
        }

        final long durationMs = System.currentTimeMillis() - state[0];

        try {
            if (jobException != null) {
                client.trackFailure(runId, durationMs, jobException.getCause() != null
                        ? jobException.getCause()
                        : jobException);
            } else {
                client.trackSuccess(runId, durationMs);
            }
        } catch (Exception e) {
            LOG.debug("TraceStaxJobListener.jobWasExecuted: unexpected exception", e);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String jobKeyString(final JobExecutionContext context) {
        final JobKey key = context.getJobDetail().getKey();
        return key.getGroup() + "." + key.getName();
    }
}
