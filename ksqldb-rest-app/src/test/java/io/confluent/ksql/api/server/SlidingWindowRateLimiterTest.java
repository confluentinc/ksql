package io.confluent.ksql.api.server;

import io.confluent.ksql.util.KsqlException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SlidingWindowRateLimiterTest {
    private SlidingWindowRateLimiter limiter;

    @Before
    public void setUp() {
        limiter = new SlidingWindowRateLimiter(1, 5 * 1000);
    }

    @Test
    public void shouldWork() {
        Throwable exception = assertThrows(KsqlException.class, () -> {
            limiter.add(0, 1148576);
            limiter.allow(1000);
        });
        assertEquals("Host is at bandwidth rate limit for pull queries.",
                exception.getMessage());
    }

    @Test
    public void shouldWork2() {
        try {
            for (int i = 0; i <= 30; i += 1) {
                limiter.add(i * 500, 100000);
                limiter.allow(i * 500 + 1);
            }
        } catch (Exception e) {
            fail("this test should not throw an exception");
        }
    }

    @Test
    public void shouldWork3() {
        Throwable exception = assertThrows(KsqlException.class, () -> {
            for (int i = 0; i <= 15; i += 1) {
                limiter.add(i * 400, 100000);
                limiter.allow(i * 400 + 1);
            }
        });

        assertEquals("Host is at bandwidth rate limit for pull queries.",
                exception.getMessage());
    }
}
