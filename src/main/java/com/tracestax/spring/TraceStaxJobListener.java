package com.tracestax.spring;

import com.tracestax.TraceStaxClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Spring Batch {@link JobExecutionListener} that automatically tracks job
 * lifecycle events with TraceStax.
 *
 * <p>Register it on your job definition:
 *
 * <pre>{@code
 * TraceStaxClient client = new TraceStaxClient("ts_live_xxxx");
 *
 * @Bean
 * public Job reportJob(JobRepository jobRepository, Step step) {
 *     return new JobBuilder("reportJob", jobRepository)
 *         .listener(new TraceStaxJobListener(client))
 *         .start(step)
 *         .build();
 * }
 * }</pre>
 *
 * <p>The listener sends a {@code task.started} event in {@link #beforeJob} and
 * either {@code task.succeeded} or {@code task.failed} in {@link #afterJob},
 * depending on the batch status. Errors in the listener itself are caught and
 * logged at DEBUG level so the SDK never destabilises the host application.
 */
public final class TraceStaxJobListener implements JobExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(TraceStaxJobListener.class);

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

    /**
     * Called by Spring Batch immediately before the job executes.
     * Sends a {@code task.started} event to TraceStax.
     */
    @Override
    public void beforeJob(final JobExecution jobExecution) {
        try {
            client.trackStart(
                    Long.toString(jobExecution.getId()),
                    jobExecution.getJobInstance().getJobName(),
                    "batch"
            );
        } catch (Exception e) {
            LOG.debug("TraceStaxJobListener.beforeJob: unexpected exception", e);
        }
    }

    /**
     * Called by Spring Batch after the job completes (successfully or not).
     * Sends either a {@code task.succeeded} or {@code task.failed} event.
     */
    @Override
    public void afterJob(final JobExecution jobExecution) {
        try {
            final long duration = Duration.between(
                    jobExecution.getStartTime(),
                    jobExecution.getEndTime() != null ? jobExecution.getEndTime() : LocalDateTime.now()
            ).toMillis();

            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                client.trackSuccess(Long.toString(jobExecution.getId()), duration);
            } else {
                // Extract the first failure from step executions
                final Throwable error = jobExecution.getAllFailureExceptions()
                        .stream()
                        .findFirst()
                        .orElse(null);
                client.trackFailure(Long.toString(jobExecution.getId()), duration, error);
            }
        } catch (Exception e) {
            LOG.debug("TraceStaxJobListener.afterJob: unexpected exception", e);
        }
    }
}
