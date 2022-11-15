/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.ksql.api.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SniHandlerTest {

  @Mock
  private HttpConnection httpConnection;
  @Mock
  private HttpServerRequest serverRequest;
  @Mock
  private RoutingContext routingContext;

  private SniHandler sniHandler;

  @Before
  public void setUp() {
    when(routingContext.request()).thenReturn(serverRequest);
    when(serverRequest.connection()).thenReturn(httpConnection);
    when(serverRequest.isSSL()).thenReturn(true);
    when(serverRequest.host()).thenReturn("");
    when(httpConnection.indicatedServerName()).thenReturn("");
    sniHandler = new SniHandler();
  }

  @Test
  public void shouldCheckOnlyIfTls() {
    // Given:
    when(routingContext.request()).thenReturn(serverRequest);
    when(serverRequest.isSSL()).thenReturn(false);

    // When:
    new SniHandler().handle(routingContext);

    // Then:
    verify(routingContext, never()).fail(anyInt(), any());
    verify(routingContext).next();
  }

  @Test
  public void shouldNotCheckIfSniNull() {
    // Given:
    when(httpConnection.indicatedServerName()).thenReturn(null);

    // When:
    sniHandler.handle(routingContext);

    // Then:
    verify(routingContext, never()).fail(anyInt(), any());
    verify(routingContext, times(1)).next();
  }

  @Test
  public void shouldNotCheckIfHostNull() {
    // Given:
    when(serverRequest.host()).thenReturn(null);

    // When:
    sniHandler.handle(routingContext);

    // Then:
    verify(routingContext, never()).fail(anyInt(), any());
    verify(routingContext, times(1)).next();
  }

  @Test
  public void shouldReturnMisdirectedResponse() {
    // Given:
    when(serverRequest.host()).thenReturn("localhost");
    when(httpConnection.indicatedServerName()).thenReturn("anotherhost");

    // When:
    sniHandler.handle(routingContext);

    // Then:
    verify(routingContext, never()).next();
    verify(routingContext).fail(anyInt(), any());
  }

  @Test
  public void shouldReturnMisdirectedResponseEvenIfPortInHost() {
    // Given:
    when(serverRequest.host()).thenReturn("localhost:80");
    when(httpConnection.indicatedServerName()).thenReturn("anotherhost");

    // When:
    sniHandler.handle(routingContext);

    // Then:
    verify(routingContext).fail(anyInt(), any());
    verify(routingContext, never()).next();
  }

  @Test
  public void shouldNotReturnMisdirectedResponseIfMatch() {
    // Given:
    when(serverRequest.host()).thenReturn("localhost");
    when(httpConnection.indicatedServerName()).thenReturn("localhost");

    // When:
    sniHandler.handle(routingContext);

    // Then:
    verify(routingContext, never()).fail(anyInt(), any());
    verify(routingContext, times(1)).next();
  }

  @Test
  public void shouldNotReturnMisdirectedResponseIfMatchHostPort() {
    // Given:
    when(serverRequest.host()).thenReturn("localhost:80");
    when(httpConnection.indicatedServerName()).thenReturn("localhost");

    // When:
    sniHandler.handle(routingContext);

    // Then:
    verify(routingContext, never()).fail(anyInt(), any());
    verify(routingContext, times(1)).next();
  }
}
