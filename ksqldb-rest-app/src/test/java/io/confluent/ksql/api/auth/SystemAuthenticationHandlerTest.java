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
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Optional;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SystemAuthenticationHandlerTest {

  @Mock private Server server;
  @Mock private RoutingContext routingContext;
  @Mock private HttpServerRequest request;
  @Mock private HttpConnection connection;
  @Mock private SSLSession sslSession;
  @Mock private Principal principal;

  @Test
  public void shouldNotBeInstalled_noListener() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of()));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, false);
    assertThat(handler.isPresent(), is(false));
  }

  @Test
  public void shouldNotBeInstalled_notMutualAuth() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://localhost:9188",
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
        KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE
    )));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, true);
    assertThat(handler.isPresent(), is(false));
  }

  @Test
  public void shouldNotBeInstalled_notInternal() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://localhost:9188",
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
        KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED
    )));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, false);
    assertThat(handler.isPresent(), is(false));
  }

  @Test
  public void shouldBeInstalled() {
    when(server.getConfig()).thenReturn(new KsqlRestConfig(ImmutableMap.of(
        KsqlRestConfig.INTERNAL_LISTENER_CONFIG, "https://localhost:9188",
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG,
        KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED
    )));
    Optional<SystemAuthenticationHandler> handler =
        SystemAuthenticationHandler.getSystemAuthenticationHandler(server, true);
    assertThat(handler.isPresent(), is(true));
  }

  @Test
  public void shouldSetUser_validSsl() throws SSLPeerUnverifiedException {
    when(routingContext.request()).thenReturn(request);
    when(request.connection()).thenReturn(connection);
    when(connection.isSsl()).thenReturn(true);
    when(connection.sslSession()).thenReturn(sslSession);
    when(sslSession.getPeerPrincipal()).thenReturn(principal);
    SystemAuthenticationHandler handler = new SystemAuthenticationHandler();
    handler.handle(routingContext);
    verify(routingContext).setUser(any());
    verify(routingContext).next();
  }

  @Test (expected = IllegalStateException.class)
  public void shouldNotSetUser_noSsl() {
    when(routingContext.request()).thenReturn(request);
    when(request.connection()).thenReturn(connection);
    when(connection.isSsl()).thenReturn(false);
    SystemAuthenticationHandler handler = new SystemAuthenticationHandler();
    handler.handle(routingContext);
    verify(routingContext, never()).setUser(any());
    verify(routingContext).next();
  }

  @Test (expected = IllegalStateException.class)
  public void houldNotSetUser_unVerifiedPeer() throws SSLPeerUnverifiedException {
    when(routingContext.request()).thenReturn(request);
    when(request.connection()).thenReturn(connection);
    when(connection.isSsl()).thenReturn(true);
    when(connection.sslSession()).thenReturn(sslSession);
    when(sslSession.getPeerPrincipal()).thenThrow(new SSLPeerUnverifiedException("Not verified"));
    SystemAuthenticationHandler handler = new SystemAuthenticationHandler();
    handler.handle(routingContext);
    verify(routingContext, never()).setUser(any());
    verify(routingContext).next();
  }
}
