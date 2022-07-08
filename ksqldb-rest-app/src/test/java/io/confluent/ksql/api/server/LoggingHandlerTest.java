package io.confluent.ksql.api.server;

import static io.confluent.ksql.api.server.LoggingHandler.HTTP_HEADER_USER_AGENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.HttpVersion;
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
public class LoggingHandlerTest {

  @Mock
  private Server server;
  @Mock
  private LoggingRateLimiter loggingRateLimiter;
  @Mock
  private Logger logger;
  @Mock
  private RoutingContext routingContext;
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

  private KsqlRestConfig config = new KsqlRestConfig(ImmutableMap.of());
  private LoggingHandler loggingHandler;


  @Before
  public void setUp() {
    when(routingContext.request()).thenReturn(request);
    when(request.response()).thenReturn(response);
    when(request.remoteAddress()).thenReturn(socketAddress);
    when(loggingRateLimiter.shouldLog(logger, "/query", 200)).thenReturn(true);
    when(loggingRateLimiter.shouldLog(logger, "/query", 405)).thenReturn(true);
    when(clock.millis()).thenReturn(1699813434333L);
    when(response.bytesWritten()).thenReturn(5678L);
    when(request.path()).thenReturn("/query");
    when(request.uri()).thenReturn("/query?foo=bar");
    when(request.getHeader(HTTP_HEADER_USER_AGENT)).thenReturn("bot");
    when(socketAddress.host()).thenReturn("123.111.222.333");
    when(request.bytesRead()).thenReturn(3456L);
    when(request.version()).thenReturn(HttpVersion.HTTP_1_1);
    when(request.method()).thenReturn(HttpMethod.POST);
    when(server.getConfig()).thenReturn(config);
    loggingHandler = new LoggingHandler(server, loggingRateLimiter, logger, clock);
  }

  @Test
  public void shouldProduceLog() {
    // Given:
    when(response.getStatusCode()).thenReturn(200);

    // When:
    loggingHandler.handle(routingContext);
    verify(routingContext).addEndHandler(endCallback.capture());
    endCallback.getValue().handle(null);

    // Then:
    verify(logger).info(logStringCaptor.capture());
    assertThat(logStringCaptor.getValue(),
        is("123.111.222.333 - - [Sun, 12 Nov 2023 18:23:54 GMT] "
            + "\"POST /query HTTP/1.1\" 200 5678 \"-\" \"bot\" 3456"));
  }

  @Test
  public void shouldProduceLogWithQuery() {
    // Given:
    when(response.getStatusCode()).thenReturn(200);
    config = new KsqlRestConfig(
        ImmutableMap.of(KsqlRestConfig.KSQL_ENDPOINT_LOGGING_LOG_QUERIES_CONFIG, true)
    );
    when(server.getConfig()).thenReturn(config);
    loggingHandler = new LoggingHandler(server, loggingRateLimiter, logger, clock);

    // When:
    loggingHandler.handle(routingContext);
    verify(routingContext).addEndHandler(endCallback.capture());
    endCallback.getValue().handle(null);

    // Then:
    verify(logger).info(logStringCaptor.capture());
    assertThat(logStringCaptor.getValue(),
        is("123.111.222.333 - - [Sun, 12 Nov 2023 18:23:54 GMT] "
            + "\"POST /query?foo=bar HTTP/1.1\" 200 5678 \"-\" \"bot\" 3456"));
  }

  @Test
  public void shouldProduceLog_warn() {
    // Given:
    when(response.getStatusCode()).thenReturn(405);

    // When:
    loggingHandler.handle(routingContext);
    verify(routingContext).addEndHandler(endCallback.capture());
    endCallback.getValue().handle(null);

    // Then:
    verify(logger).warn(logStringCaptor.capture());
    assertThat(logStringCaptor.getValue(),
        is("123.111.222.333 - - [Sun, 12 Nov 2023 18:23:54 GMT] "
            + "\"POST /query HTTP/1.1\" 405 5678 \"-\" \"bot\" 3456"));
  }

  @Test
  public void shouldSkipLog() {
    // Given:
    when(response.getStatusCode()).thenReturn(401);
    when(loggingRateLimiter.shouldLog(logger, "/query", 401)).thenReturn(false);

    // When:
    loggingHandler.handle(routingContext);
    verify(routingContext).addEndHandler(endCallback.capture());
    endCallback.getValue().handle(null);

    // Then:
    verify(logger, never()).info(any());
    verify(logger, never()).warn(any());
    verify(logger, never()).error(any());
  }

  @Test
  public void shouldSkipLogIfFiltered() {
    // Given:
    when(response.getStatusCode()).thenReturn(200);
    config = new KsqlRestConfig(
        ImmutableMap.of(KsqlRestConfig.KSQL_ENDPOINT_LOGGING_IGNORED_PATHS_REGEX_CONFIG, ".*query.*")
    );
    when(server.getConfig()).thenReturn(config);
    loggingHandler = new LoggingHandler(server, loggingRateLimiter, logger, clock);

    // When:
    loggingHandler.handle(routingContext);
    verify(routingContext).addEndHandler(endCallback.capture());
    endCallback.getValue().handle(null);

    // Then:
    verify(logger, never()).info(any());
    verify(logger, never()).warn(any());
    verify(logger, never()).error(any());
  }

  @Test
  public void shouldProduceLogWithRandomFilter() {
    // Given:
    when(response.getStatusCode()).thenReturn(200);
    config = new KsqlRestConfig(
        ImmutableMap.of(KsqlRestConfig.KSQL_ENDPOINT_LOGGING_IGNORED_PATHS_REGEX_CONFIG, ".*random.*")
    );
    when(server.getConfig()).thenReturn(config);
    loggingHandler = new LoggingHandler(server, loggingRateLimiter, logger, clock);

    // When:
    loggingHandler.handle(routingContext);
    verify(routingContext).addEndHandler(endCallback.capture());
    endCallback.getValue().handle(null);

    // Then:
    verify(logger).info(logStringCaptor.capture());
    assertThat(logStringCaptor.getValue(),
        is("123.111.222.333 - - [Sun, 12 Nov 2023 18:23:54 GMT] "
            + "\"POST /query HTTP/1.1\" 200 5678 \"-\" \"bot\" 3456"));
  }

  @Test
  public void shouldSkipRateLimited() {
    // Given:
    when(response.getStatusCode()).thenReturn(200);
    when(loggingRateLimiter.shouldLog(logger, "/query", 200)).thenReturn(true, true, false, false);

    // When:
    loggingHandler.handle(routingContext);
    loggingHandler.handle(routingContext);
    loggingHandler.handle(routingContext);
    loggingHandler.handle(routingContext);
    verify(routingContext, times(4)).addEndHandler(endCallback.capture());
    for (Handler<AsyncResult<Void>> handler : endCallback.getAllValues()) {
      handler.handle(null);
    }

    // Then:
    verify(logger, times(2)).info(logStringCaptor.capture());
    for (String message : logStringCaptor.getAllValues()) {
      assertThat(message,
          is("123.111.222.333 - - [Sun, 12 Nov 2023 18:23:54 GMT] "
              + "\"POST /query HTTP/1.1\" 200 5678 \"-\" \"bot\" 3456"));
    }
  }
}
