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
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import java.time.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class LoggingRateLimiterTest {

  @Mock
  private Server server;
  @Mock
  private Logger logger;
  @Mock
  private RateLimiter rateLimiter;
  @Mock
  private RoutingContext routingContext;
  @Mock
  private KsqlRestConfig ksqlRestConfig;
  @Mock
  private HttpServerRequest request;
  @Mock
  private HttpServerResponse response;
  @Mock
  private SocketAddress socketAddress;
  @Mock
  private Clock clock;
  @Captor
  private ArgumentCaptor<String> logStringCaptor;
  @Captor
  private ArgumentCaptor<Handler<AsyncResult<Void>>> endCallback;

  private LoggingRateLimiter loggingRateLimiter;


  @Before
  public void setUp() {
    when(routingContext.request()).thenReturn(request);
    when(request.path()).thenReturn("/query");
    when(ksqlRestConfig.getStringAsMap(any())).thenReturn(ImmutableMap.of("/query", "2"));
    when(rateLimiter.tryAcquire()).thenReturn(true);
    loggingRateLimiter = new LoggingRateLimiter(ksqlRestConfig, (rateLimit) -> rateLimiter);
  }

  @Test
  public void shouldLog() {
    // When:
    assertThat(loggingRateLimiter.shouldLog(routingContext), is(true));

    // Then:
    verify(rateLimiter).tryAcquire();
  }

  @Test
  public void shouldSkipRateLimited() {
    // Given:
    when(rateLimiter.tryAcquire()).thenReturn(true, true, false, false);

    // When:
    assertThat(loggingRateLimiter.shouldLog(routingContext), is(true));
    assertThat(loggingRateLimiter.shouldLog(routingContext), is(true));
    assertThat(loggingRateLimiter.shouldLog(routingContext), is(false));
    assertThat(loggingRateLimiter.shouldLog(routingContext), is(false));

    // Then:
    verify(rateLimiter, times(4)).tryAcquire();
  }

  @Test
  public void shouldLog_notIncluded() {
    // When:
    when(request.path()).thenReturn("/foo");
    assertThat(loggingRateLimiter.shouldLog(routingContext), is(true));

    // Then:
    verify(rateLimiter, never()).tryAcquire();
  }
}
