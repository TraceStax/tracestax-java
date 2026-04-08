# TraceStax Java SDK

The official Java SDK for the [TraceStax](https://tracestax.com) Worker Intelligence Platform. TraceStax collects job and task lifecycle events from background job frameworks so you can monitor queue depths, worker health, and job failure rates in one place.

## Requirements

- Java 17+
- Maven 3.6+ (or Gradle 7+)

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.tracestax</groupId>
    <artifactId>tracestax-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

Or with Gradle:

```groovy
implementation 'com.tracestax:tracestax-java:0.1.0'
```

## Basic Setup

Create a single `TraceStaxClient` instance for the lifetime of your application, then call `shutdown()` when the application stops.

```java
import com.tracestax.TraceStaxClient;

TraceStaxClient client = new TraceStaxClient(System.getenv("TRACESTAX_API_KEY"));

// At application shutdown:
client.shutdown();
```

All HTTP calls are fire-and-forget: they run on a daemon background thread and never throw exceptions back to the caller.

## Plain Java - Manual Tracking

Use `TraceStaxClient` directly when you want full control over event timing.

```java
import com.tracestax.TraceStaxClient;
import java.util.UUID;

TraceStaxClient client = new TraceStaxClient(System.getenv("TRACESTAX_API_KEY"));

String runId = UUID.randomUUID().toString();
client.trackStart(runId, "com.example.ReportJob", "reports");

long start = System.currentTimeMillis();
try {
    // ... execute job logic ...
    client.trackSuccess(runId, System.currentTimeMillis() - start);
} catch (Exception e) {
    client.trackFailure(runId, System.currentTimeMillis() - start, e);
    throw e;
}
```

### Heartbeats and Snapshots

```java
// Send a heartbeat from a worker process
client.heartbeat("worker-pod-3", new String[]{"reports", "emails"}, 8);

// Send a queue-depth snapshot (one call per queue)
client.snapshot("reports", 42, null, null);
client.snapshot("emails",   7, null, null);
```

## Quartz Scheduler Integration

`TraceStaxJobListener` implements Quartz's `JobListener` interface. Register it once against your scheduler and it will automatically track every job.

```java
import com.tracestax.TraceStaxClient;
import com.tracestax.quartz.TraceStaxJobListener;
import org.quartz.Scheduler;

TraceStaxClient client = new TraceStaxClient(System.getenv("TRACESTAX_API_KEY"));

Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
scheduler.getListenerManager().addJobListener(new TraceStaxJobListener(client));
scheduler.start();
```

The listener maps Quartz concepts to TraceStax fields as follows:

| TraceStax field | Quartz source |
|---|---|
| `run_id` | generated UUID per execution |
| `job_class` | `JobDetail.getJobClass().getName()` |
| `queue` | `JobDetail.getKey().getGroup()` |
| `duration_ms` | wall-clock time between `jobToBeExecuted` and `jobWasExecuted` |
| `error.class` | `JobExecutionException.getCause().getClass().getName()` |
| `error.message` | `JobExecutionException.getCause().getMessage()` |

To monitor only a subset of jobs, pass a matcher:

```java
import org.quartz.impl.matchers.GroupMatcher;

scheduler.getListenerManager().addJobListener(
    new TraceStaxJobListener(client),
    GroupMatcher.jobGroupEquals("critical")
);
```

## Spring Boot Integration

### 1. Configure beans

```java
import com.tracestax.TraceStaxClient;
import com.tracestax.spring.TraceStaxAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy
public class TraceStaxConfig {

    @Bean
    public TraceStaxClient tracestaxClient() {
        return new TraceStaxClient(System.getenv("TRACESTAX_API_KEY"));
    }

    @Bean
    public TraceStaxAspect tracestaxAspect(TraceStaxClient client) {
        return new TraceStaxAspect(client);
    }
}
```

### 2. Annotate job methods

```java
import com.tracestax.spring.TraceStaxMonitor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ReportGenerator {

    @Scheduled(cron = "0 0 * * * *")
    @TraceStaxMonitor(jobClass = "com.example.ReportGenerator", queue = "reports")
    public void generateMonthlyReport() {
        // business logic here - TraceStax events are sent automatically
    }
}
```

The `jobClass` and `queue` attributes are both optional:

- If `jobClass` is omitted, the fully-qualified name of the declaring class is used.
- If `queue` is omitted, the field is not sent in the event payload.

### 3. Graceful shutdown

Spring manages the bean lifecycle. To ensure the SDK flushes pending events on shutdown, implement `DisposableBean` or use `@PreDestroy`:

```java
import javax.annotation.PreDestroy;

@Configuration
public class TraceStaxConfig {

    private TraceStaxClient tracestaxClient;

    @Bean
    public TraceStaxClient tracestaxClient() {
        tracestaxClient = new TraceStaxClient(System.getenv("TRACESTAX_API_KEY"));
        return tracestaxClient;
    }

    @PreDestroy
    public void onShutdown() {
        tracestaxClient.shutdown();
    }
}
```

## Configuration Options

| Option | Type | Default | Description |
|---|---|---|---|
| `apiKey` | `String` | - | **Required.** TraceStax API key. Use `TRACESTAX_API_KEY` env var by convention. |
| `endpoint` | `String` | `https://ingest.tracestax.com` | Base URL for the TraceStax ingest API. Override for self-hosted deployments or testing. |

## Authentication

The SDK sends your API key in two headers on every request. Either is accepted by the TraceStax API:

```
Authorization: Bearer <api_key>
X-Api-Key:     <api_key>
```

## API Endpoints Used

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/ingest` | Task lifecycle events (`task.started`, `task.succeeded`, `task.failed`) |
| `POST` | `/v1/heartbeat` | Worker heartbeat |
| `POST` | `/v1/snapshot` | Queue depth snapshot |

## Error Handling

The SDK is designed to be invisible in production. All HTTP errors, network failures, and serialisation problems are caught internally and logged at `DEBUG` level via SLF4J. No exception is ever propagated to caller code.

To see SDK debug output, configure your logging framework:

```xml
<!-- Logback example -->
<logger name="com.tracestax" level="DEBUG"/>
```

## License

MIT
