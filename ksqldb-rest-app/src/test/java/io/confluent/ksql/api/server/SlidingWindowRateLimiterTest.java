package io.confluent.ksql.api.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SlidingWindowRateLimiterTest {
    private static final String RATE_LIMIT_MESSAGE =
        "Host is at bandwidth rate limit for pull queries.";
    private static final String TEST_SHOULD_NOT_FAIL = "This test should not throw an exception";
    private static final String METRIC_PREFIX = "test";

    @Test
    public void addingToTheLimiter() {
        final Metrics metrics = new Metrics();
        final Map<String, String> tags = Collections.emptyMap();
        final SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(
            1,
            5L * 1000L,
            METRIC_PREFIX,
            metrics,
            tags
        );

        assertEquals(1048576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        final Throwable exception = assertThrows(KsqlException.class, () -> {
            limiter.add(0L, 1148576L);
            limiter.allow(KsqlQueryType.PULL, 1000L);
        });
        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());

        assertEquals(-100000.0, getRemaining(metrics, tags), 0.0);
        assertEquals(1.0, getReject(metrics, tags), 0.0);
    }

    @Test
    public void bigInitialResponse() {
        final Metrics metrics = new Metrics();
        final Map<String, String> tags = Collections.emptyMap();
        final SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(
            1,
            5L * 1000L,
            METRIC_PREFIX,
            metrics,
            tags
        );

        assertEquals(1048576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        final Throwable exception = assertThrows(KsqlException.class, () -> {
            limiter.add(0L, 1148576L);
            limiter.allow(KsqlQueryType.PULL, 1000L);
        });
        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());

        assertEquals(-100000.0, getRemaining(metrics, tags), 0.0);
        assertEquals(1.0, getReject(metrics, tags), 0.0);
    }

    @Test
    public void uniformResponsesUnderLimit() {
        final Metrics metrics = new Metrics();
        final Map<String, String> tags = Collections.emptyMap();
        final SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(
            1,
            5L * 1000L,
            METRIC_PREFIX,
            metrics,
            tags
        );

        assertEquals(1048576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        try {
            for (long i = 0L; i < 30L; i += 1L) {
                limiter.add(i * 500L, 100000L);
                limiter.allow(KsqlQueryType.PULL, i * 500L + 1L);
            }
        } catch (final Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        assertEquals(48576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);
    }

    @Test
    public void uniformResponsesOverLimit() {
        final Metrics metrics = new Metrics();
        final Map<String, String> tags = Collections.emptyMap();
        final SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(
            1,
            5L * 1000L,
            METRIC_PREFIX,
            metrics,
            tags
        );

        assertEquals(1048576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        final Throwable exception = assertThrows(KsqlException.class, () -> {
            for (long i = 0L; i < 11L; i += 1L) {
                limiter.add(i * 400L, 100000L);
                limiter.allow(KsqlQueryType.PULL, i * 400L + 1L);
            }
        });

        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());

        assertEquals(-51424.0, getRemaining(metrics, tags), 0.0);
        assertEquals(1.0, getReject(metrics, tags), 0.0);
    }

    @Test
    public void justUnderForAWhileThenOverLimit() {
        final Metrics metrics = new Metrics();
        final Map<String, String> tags = Collections.emptyMap();
        final SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(
            1,
            5L * 1000L,
            METRIC_PREFIX,
            metrics,
            tags
        );

        assertEquals(1048576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        try {
            for (long i = 0L; i < 5L; i += 1L) {
                limiter.add(i * 500L, i * 100000L);
                limiter.allow(KsqlQueryType.PULL, i * 500L + 1L);
            }
        } catch (final Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        assertEquals(48576.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        try {
            limiter.allow(KsqlQueryType.PULL, 3499L);
            limiter.add(3500L, 80000L);
        } catch (final Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        assertEquals(-31424.0, getRemaining(metrics, tags), 0.0);
        assertEquals(0.0, getReject(metrics, tags), 0.0);

        final Throwable exception = assertThrows(KsqlException.class, () -> {
            for (long i = 10L; i < 18L; i += 1L) {
                limiter.add(i * 600L, 140000L);
                limiter.allow(KsqlQueryType.PULL,i * 600L + 1L);
            }
        });

        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());

        assertEquals(-71424.0, getRemaining(metrics, tags), 0.0);
        assertEquals(1.0, getReject(metrics, tags), 0.0);
    }

    private double getReject(final Metrics metrics, final Map<String, String> tags) {
        final MetricName rejectMetricName = new MetricName(
            METRIC_PREFIX + "-bandwidth-limit-reject-count",
            "_confluent-ksql-limits",
            "The number of requests rejected by this limiter",
            tags
        );
        final KafkaMetric rejectMetric = metrics.metrics().get(rejectMetricName);
        return (double) rejectMetric.metricValue();
    }

    private double getRemaining(final Metrics metrics, final Map<String, String> tags) {
        final MetricName remainingMetricName = new MetricName(
            METRIC_PREFIX + "-bandwidth-limit-remaining",
            "_confluent-ksql-limits",
            "The current value of the bandwidth limiter",
            tags
        );
        final KafkaMetric remainingMetric = metrics.metrics().get(remainingMetricName);
        return (double) remainingMetric.metricValue();
    }
}
