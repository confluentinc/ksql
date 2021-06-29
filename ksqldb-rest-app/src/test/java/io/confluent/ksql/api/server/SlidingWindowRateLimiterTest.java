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
        limiter = new SlidingWindowRateLimiter(1, 5 * 1000);
    }

    @Test
    public void bigInitialResponse() {
        Throwable exception = assertThrows(KsqlException.class, () -> {
            limiter.add(0, 1148576);
            limiter.allow(1000);
        });
        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());
    }

    @Test
    public void uniformResponsesUnderLimit() {
        try {
            for (int i = 0; i < 30; i += 1) {
                limiter.add(i * 500, 100000);
                limiter.allow(i * 500 + 1);
            }
        } catch (Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }
    }

    @Test
    public void uniformResponsesOverLimit() {
        Throwable exception = assertThrows(KsqlException.class, () -> {
            for (int i = 0; i < 11; i += 1) {
                limiter.add(i * 400, 100000);
                limiter.allow(i * 400 + 1);
            }
        });

        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());
    }

    @Test
    public void justUnderForAWhileThenOverLimit() {
        try {
            for (int i = 0; i < 5; i += 1) {
                limiter.add(i * 500, i * 100000);
                limiter.allow(i * 500 + 1);
            }
        } catch (Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        try {
            limiter.allow(3499);
            limiter.add(3500, 80000);
        } catch (Exception e) {
            fail(TEST_SHOULD_NOT_FAIL);
        }

        Throwable exception = assertThrows(KsqlException.class, () -> {
            for (int i = 10; i < 18; i += 1) {
                limiter.add(i * 600, 140000);
                limiter.allow(i * 600 + 1);
            }
        });

        assertEquals(RATE_LIMIT_MESSAGE, exception.getMessage());
    }
}
