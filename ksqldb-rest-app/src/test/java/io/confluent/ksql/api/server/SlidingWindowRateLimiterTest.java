package io.confluent.ksql.api.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SlidingWindowRateLimiterTest {
    private SlidingWindowRateLimiter limiter;
    private static String RATE_LIMIT_MESSAGE = "Host is at bandwidth rate limit for pull queries.";
    private static String TEST_SHOULD_NOT_FAIL = "This test should not throw an exception";

    @Before
    public void setUp() {
        limiter = new SlidingWindowRateLimiter(1, 5L * 1000L);
    }

    @Test
    public void bigInitialResponse() {
        Throwable exception = assertThrows(KsqlException.class, () -> {
            limiter.add(0L, 1148576L);
            limiter.allow(1000L);
        });
        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());
    }

    @Test
    public void uniformResponsesUnderLimit() {
        try {
            for (long i = 0L; i < 30L; i += 1L) {
                limiter.add(i * 500L, 100000L);
                limiter.allow(i * 500L + 1L);
            }
        } catch (Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }
    }

    @Test
    public void uniformResponsesOverLimit() {
        Throwable exception = assertThrows(KsqlException.class, () -> {
            for (long i = 0L; i < 11L; i += 1L) {
                limiter.add(i * 400L, 100000L);
                limiter.allow(i * 400L + 1L);
            }
        });

        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());
    }

    @Test
    public void justUnderForAWhileThenOverLimit() {
        try {
            for (long i = 0L; i < 5L; i += 1L) {
                limiter.add(i * 500L, i * 100000L);
                limiter.allow(i * 500L + 1L);
            }
        } catch (Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        try {
            limiter.allow(3499L);
            limiter.add(3500L, 80000L);
        } catch (Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        Throwable exception = assertThrows(KsqlException.class, () -> {
            for (long i = 10L; i < 18L; i += 1L) {
                limiter.add(i * 600L, 140000L);
                limiter.allow(i * 600L + 1L);
            }
        });

        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());
    }
}
