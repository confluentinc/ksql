package io.confluent.ksql.api.server;


import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SERVER_RATE_LIMITED_REQUEST_PATHS_CONFIG;
import static io.confluent.ksql.rest.server.KsqlRestConfig.KSQL_LOGGING_SERVER_RATE_LIMITED_RESPONSE_CODES_CONFIG;
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
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class LoggingRateLimiterTest {

  private static final String PATH = "/query";
  private static final int RESPONSE_CODE = 401;

  @Mock
  private RateLimiter rateLimiter;
  @Mock
  private RateLimiter pathLimiter;
  @Mock
  private RateLimiter responseCodeLimiter;
  @Mock
  private KsqlRestConfig ksqlRestConfig;
  @Mock
  private Logger logger;
  @Mock
  Function<Double, RateLimiter> factory;

  private LoggingRateLimiter loggingRateLimiter;


  @Before
  public void setUp() {
    when(ksqlRestConfig.getStringAsMap(KSQL_LOGGING_SERVER_RATE_LIMITED_REQUEST_PATHS_CONFIG))
        .thenReturn(ImmutableMap.of(PATH, "2"));
    when(ksqlRestConfig.getStringAsMap(KSQL_LOGGING_SERVER_RATE_LIMITED_RESPONSE_CODES_CONFIG))
        .thenReturn(ImmutableMap.of(Integer.toString(RESPONSE_CODE), "1"));
    when(rateLimiter.tryAcquire()).thenReturn(true);
    when(factory.apply(any())).thenReturn(pathLimiter, responseCodeLimiter, rateLimiter);
    when(pathLimiter.tryAcquire()).thenReturn(true);
    when(responseCodeLimiter.tryAcquire()).thenReturn(true);
    loggingRateLimiter = new LoggingRateLimiter(ksqlRestConfig, factory);
  }

  @Test
  public void shouldLog() {
    // When:
    assertThat(loggingRateLimiter.shouldLog(logger, PATH, 200), is(true));

    // Then:
    verify(rateLimiter).tryAcquire();
    verify(logger, never()).info(any());
  }

  @Test
  public void shouldSkipRateLimited_path() {
    // Given:
    when(rateLimiter.tryAcquire()).thenReturn(true, true, false, false);
    when(rateLimiter.getRate()).thenReturn(2d);

    // When:
    assertThat(loggingRateLimiter.shouldLog(logger, PATH, 200), is(true));
    assertThat(loggingRateLimiter.shouldLog(logger, PATH, 200), is(true));
    assertThat(loggingRateLimiter.shouldLog(logger, PATH, 200), is(false));
    assertThat(loggingRateLimiter.shouldLog(logger, PATH, 200), is(false));

    // Then:
    verify(rateLimiter, times(4)).tryAcquire();
    verify(logger, times(2)).info("Hit rate limit for path /query with limit 2.0");
  }

  @Test
  public void shouldSkipRateLimited_responseCode() {
    // Given:
    when(rateLimiter.tryAcquire()).thenReturn(true, false, false, false);
    when(rateLimiter.getRate()).thenReturn(1d);

    // When:
    assertThat(loggingRateLimiter.shouldLog(logger, "/foo", RESPONSE_CODE), is(true));
    assertThat(loggingRateLimiter.shouldLog(logger, "/foo", RESPONSE_CODE), is(false));
    assertThat(loggingRateLimiter.shouldLog(logger, "/foo", RESPONSE_CODE), is(false));
    assertThat(loggingRateLimiter.shouldLog(logger, "/foo", RESPONSE_CODE), is(false));

    // Then:
    verify(rateLimiter, times(4)).tryAcquire();
    verify(logger, times(3)).info("Hit rate limit for response code 401 with limit 1.0");
  }

  @Test
  public void shouldLog_notRateLimited() {
    // When:
    assertThat(loggingRateLimiter.shouldLog(logger, "/foo", 200), is(true));

    // Then:
    verify(rateLimiter, never()).tryAcquire();
    verify(logger, never()).info(any());
  }
}
