package io.confluent.ksql.api.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SystemAuthenticationHandlerTest {

  @Mock Server server;
  @Mock RoutingContext routingContext;

  private SystemAuthenticationHandler systemAuthenticationHandler;

  @Before
  public void setUp() throws Exception {

  }

  @Test
  public void noListener() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of()));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, Optional.empty());
    assertThat(handler.isPresent(), is(false));
  }

  @Test
  public void notMutualAuth() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://localhost:9188",
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
        KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE
    )));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, Optional.of(true));
    assertThat(handler.isPresent(), is(false));
  }

  @Test
  public void notInternal() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://localhost:9188",
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
        KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED
    )));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, Optional.of(false));
    assertThat(handler.isPresent(), is(false));
  }

  @Test
  public void success() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://localhost:9188",
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
        KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED
    )));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, Optional.of(true));
    assertThat(handler.isPresent(), is(true));
  }

  @Test
  public void handle_validPath() {
    when(routingContext.normalisedPath()).thenReturn("/heartbeat");
    SystemAuthenticationHandler handler = new SystemAuthenticationHandler();
    handler.handle(routingContext);
    verify(routingContext).setUser(any());
    verify(routingContext).next();
  }

  @Test
  public void handle_invalidPath() {
    when(routingContext.normalisedPath()).thenReturn("/foo");
    SystemAuthenticationHandler handler = new SystemAuthenticationHandler();
    handler.handle(routingContext);
    verify(routingContext, never()).setUser(any());
    verify(routingContext).next();
  }
}
