package io.confluent.ksql.api.server;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoggingRateLimiterTest {

  private static final String PATH = "/query";

  @Mock
  private RateLimiter rateLimiter;
  @Mock
  private KsqlRestConfig ksqlRestConfig;

  private LoggingRateLimiter loggingRateLimiter;


  @Before
  public void setUp() {
    when(ksqlRestConfig.getStringAsMap(any())).thenReturn(ImmutableMap.of(PATH, "2"));
    when(rateLimiter.tryAcquire()).thenReturn(true);
    loggingRateLimiter = new LoggingRateLimiter(ksqlRestConfig, (rateLimit) -> rateLimiter);
  }

  @Test
  public void shouldLog() {
    // When:
    assertThat(loggingRateLimiter.shouldLog(PATH), is(true));

    // Then:
    verify(rateLimiter).tryAcquire();
  }

  @Test
  public void shouldSkipRateLimited() {
    // Given:
    when(rateLimiter.tryAcquire()).thenReturn(true, true, false, false);

    // When:
    assertThat(loggingRateLimiter.shouldLog(PATH), is(true));
    assertThat(loggingRateLimiter.shouldLog(PATH), is(true));
    assertThat(loggingRateLimiter.shouldLog(PATH), is(false));
    assertThat(loggingRateLimiter.shouldLog(PATH), is(false));

    // Then:
    verify(rateLimiter, times(4)).tryAcquire();
  }

  @Test
  public void shouldLog_notRateLimited() {
    // When:
    assertThat(loggingRateLimiter.shouldLog("/foo"), is(true));

    // Then:
    verify(rateLimiter, never()).tryAcquire();
  }
}
