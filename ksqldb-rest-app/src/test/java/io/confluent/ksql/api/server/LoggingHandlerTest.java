package io.confluent.ksql.api.server;

import static io.confluent.ksql.api.server.LoggingHandler.HTTP_HEADER_USER_AGENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoggingHandlerTest {

  @Mock
  private Server server;
  @Mock
  private Consumer<String> logger;
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
  @Captor
  private ArgumentCaptor<String> logStringCaptor;

  private LoggingHandler loggingHandler;


  @Before
  public void setUp() {
    when(server.getConfig()).thenReturn(ksqlRestConfig);
    when(routingContext.response()).thenReturn(response);
    when(routingContext.request()).thenReturn(request);
    when(request.remoteAddress()).thenReturn(socketAddress);
    when(ksqlRestConfig.getList(any())).thenReturn(ImmutableList.of("401"));
    loggingHandler = new LoggingHandler(server, logger);
  }

  @Test
  public void shouldProduceLog() {
    when(response.getStatusCode()).thenReturn(200);
    when(request.uri()).thenReturn("/query");
    when(request.getHeader(HTTP_HEADER_USER_AGENT)).thenReturn("bot");
    when(socketAddress.host()).thenReturn("123.111.222.333");
    when(request.bytesRead()).thenReturn(3456L);
    loggingHandler.logRequestEnd(routingContext);

    verify(logger).accept(logStringCaptor.capture());
    assertThat(logStringCaptor.getValue(),
        is("Request complete - 123.111.222.333: /query status: 200, "
            + "user agent: bot, request body: 3456 bytes, error response: none"));
  }

  @Test
  public void shouldSkipLog() {
    when(response.getStatusCode()).thenReturn(401);

    loggingHandler.logRequestEnd(routingContext);

    verify(logger, never()).accept(any());
  }
}
