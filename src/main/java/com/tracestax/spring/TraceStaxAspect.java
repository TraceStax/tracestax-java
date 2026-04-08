package com.tracestax.spring;

import com.tracestax.TraceStaxClient;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.UUID;

/**
 * Spring AOP aspect that intercepts methods annotated with {@link TraceStaxMonitor}
 * and sends lifecycle events to TraceStax.
 *
 * <p>Register the aspect and the {@link TraceStaxClient} as Spring beans:
 *
 * <pre>{@code
 * @Configuration
 * @EnableAspectJAutoProxy
 * public class TraceStaxConfig {
 *
 *     @Bean
 *     public TraceStaxClient tracestaxClient() {
 *         return new TraceStaxClient(System.getenv("TRACESTAX_API_KEY"));
 *     }
 *
 *     @Bean
 *     public TraceStaxAspect tracestaxAspect(TraceStaxClient client) {
 *         return new TraceStaxAspect(client);
 *     }
 * }
 * }</pre>
 *
 * <p>A fresh UUID run ID is generated for every method invocation.
 */
@Aspect
@Component
public final class TraceStaxAspect {

    private static final Logger LOG = LoggerFactory.getLogger(TraceStaxAspect.class);

    private final TraceStaxClient client;

    /**
     * @param client a configured {@link TraceStaxClient}
     */
    public TraceStaxAspect(final TraceStaxClient client) {
        if (client == null) {
            throw new IllegalArgumentException("client must not be null");
        }
        this.client = client;
    }

    /**
     * Intercepts any method annotated with {@link TraceStaxMonitor}, sends
     * lifecycle events, and re-throws any exception after recording failure.
     *
     * @param pjp     the proceeding join point provided by the AOP framework
     * @param monitor the annotation instance (for reading attributes)
     * @return the value returned by the target method
     * @throws Throwable any exception thrown by the target method
     */
    @Around("@annotation(monitor)")
    public Object monitor(final ProceedingJoinPoint pjp, final TraceStaxMonitor monitor) throws Throwable {
        final String runId = UUID.randomUUID().toString();
        final String jobClass = resolveJobClass(pjp, monitor);
        final String queue = monitor.queue().isBlank() ? null : monitor.queue();

        try {
            client.trackStart(runId, jobClass, queue);
        } catch (Exception e) {
            LOG.debug("TraceStaxAspect: trackStart failed", e);
        }

        final long start = System.currentTimeMillis();

        try {
            final Object result = pjp.proceed();
            final long duration = System.currentTimeMillis() - start;

            try {
                client.trackSuccess(runId, duration);
            } catch (Exception e) {
                LOG.debug("TraceStaxAspect: trackSuccess failed", e);
            }

            return result;

        } catch (Throwable t) {
            final long duration = System.currentTimeMillis() - start;

            try {
                client.trackFailure(runId, duration, t);
            } catch (Exception e) {
                LOG.debug("TraceStaxAspect: trackFailure failed", e);
            }

            throw t;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the job class name to report: the annotation value when provided,
     * otherwise the fully-qualified name of the declaring class.
     */
    private static String resolveJobClass(final ProceedingJoinPoint pjp, final TraceStaxMonitor monitor) {
        if (!monitor.jobClass().isBlank()) {
            return monitor.jobClass();
        }
        // Fall back to the declaring class name via the method signature.
        try {
            final MethodSignature sig = (MethodSignature) pjp.getSignature();
            final Method method = sig.getMethod();
            return method.getDeclaringClass().getName();
        } catch (Exception e) {
            LOG.debug("TraceStaxAspect: could not resolve job class from signature", e);
            return pjp.getTarget().getClass().getName();
        }
    }
}
