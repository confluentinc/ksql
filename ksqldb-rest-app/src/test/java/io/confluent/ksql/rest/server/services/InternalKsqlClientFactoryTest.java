package io.confluent.ksql.rest.server.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientAgent;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.PoolOptions;
import io.vertx.core.net.SocketAddress;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InternalKsqlClientFactoryTest {

  @Mock
  private Vertx vertx;
  @Mock
  private HttpClientAgent httpClient;
  @Captor
  private ArgumentCaptor<HttpClientOptions> options;
  @Captor
  private ArgumentCaptor<PoolOptions> poolOptions;

  @Before
  public void setUp() {
    when(vertx.createHttpClient(options.capture(), poolOptions.capture())).thenReturn(httpClient);
  }

  @Test
  public void shouldCreateClient() {
    // When:
    KsqlClient client = InternalKsqlClientFactory.createInternalClient(
        ImmutableMap.of(), SocketAddress::inetSocketAddress,
        vertx);

    // Then:
    assertThat(client, notNullValue());
  }

  @Test
  public void shouldCreateClient_http2() {
    // When:
    KsqlClient client = InternalKsqlClientFactory.createInternalClient(
        ImmutableMap.of(
            KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_CONFIG, "1234"
        ), SocketAddress::inetSocketAddress,
        vertx);
    List<PoolOptions> poolOptionsList = poolOptions.getAllValues();


    // Then:
    assertThat(client, notNullValue());
    assertThat(poolOptionsList.size(), is(4));
    PoolOptions http2PoolOptions = poolOptionsList.get(2);
    assertThat(http2PoolOptions.getHttp2MaxSize(), is(1234));
  }

  @Test
  public void shouldCreateClient_http2_badPoolSize() {
    // When:
    KsqlClient client = InternalKsqlClientFactory.createInternalClient(
        ImmutableMap.of(
            KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_CONFIG, "abc"
        ), SocketAddress::inetSocketAddress,
        vertx);
    List<PoolOptions> poolOptionsList = poolOptions.getAllValues();


    // Then:
    assertThat(client, notNullValue());
    assertThat(poolOptionsList.size(), is(4));
    PoolOptions http2PoolOptions = poolOptionsList.get(2);
    assertThat(http2PoolOptions.getHttp2MaxSize(),
        is(KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_DEFAULT));
  }
}
