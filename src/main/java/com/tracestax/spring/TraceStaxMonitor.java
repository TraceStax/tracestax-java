package com.tracestax.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method for automatic TraceStax monitoring via {@link TraceStaxAspect}.
 *
 * <p>Apply to any Spring-managed bean method to have TraceStax record
 * {@code task.started}, {@code task.succeeded}, and {@code task.failed} events
 * automatically, without modifying business logic.
 *
 * <pre>{@code
 * @Component
 * public class ReportGenerator {
 *
 *     @TraceStaxMonitor(jobClass = "com.example.ReportGenerator", queue = "reports")
 *     public void generateMonthlyReport() {
 *         // ...
 *     }
 * }
 * }</pre>
 *
 * <p>Requires {@link TraceStaxAspect} to be registered as a Spring bean and
 * {@code @EnableAspectJAutoProxy} to be present on a configuration class.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TraceStaxMonitor {

    /**
     * Logical job class name sent to TraceStax.
     *
     * <p>Defaults to the fully-qualified name of the declaring class if left blank.
     */
    String jobClass() default "";

    /**
     * Queue / topic name associated with this job.
     *
     * <p>Defaults to an empty string (omitted from the event payload) if left blank.
     */
    String queue() default "";
}
