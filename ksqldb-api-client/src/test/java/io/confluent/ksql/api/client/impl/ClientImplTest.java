/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.impl;

import static io.netty.handler.codec.http.HttpHeaderNames.USER_AGENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.RequestOptions;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClientImplTest {

  private static final ClientOptions OPTIONS_1 = ClientOptions.create();
  private static final ClientOptions OPTIONS_2 = ClientOptions.create().setUseTls(true);

  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            Client.create(OPTIONS_1)
        )
        .addEqualityGroup(
            Client.create(OPTIONS_1, vertx),
            Client.create(OPTIONS_1, vertx)
        )
        .addEqualityGroup(
            Client.create(OPTIONS_2, vertx)
        )
        .testEquals();
  }

  @Test
  public void shouldSetUserAgent() {
    // Given
    Vertx vertx = Mockito.mock(Vertx.class);
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpClientRequest clientRequest = Mockito.mock(HttpClientRequest.class);
    HashMap<String, String> headers = new HashMap<>();

    // When
    when(vertx.createHttpClient(any())).thenReturn(httpClient);
    doAnswer(a -> {
      ((Handler<AsyncResult<HttpClientRequest>>) a.getArgument(1))
          .handle(Future.succeededFuture(clientRequest));
      return null;
    }).when(httpClient).request(any(RequestOptions.class), any(Handler.class));
    when(clientRequest.exceptionHandler(any())).thenReturn(clientRequest);
    when(clientRequest.putHeader((String) any(), (String) any())).thenAnswer(a -> {
      String key = a.getArgument(0);
      String value = a.getArgument(1);
      headers.put(key, value);
      return clientRequest;
    });

    // Then
    Client client = Client.create(OPTIONS_1, vertx);
    client.streamQuery("SELECT * from STREAM1 EMIT CHANGES;");
    assertThat(headers.size(), is(1));
    assertThat(headers.containsKey(USER_AGENT.toString()), is(true));
    assertThat(headers.get(USER_AGENT.toString()).matches("ksqlDB Java Client v\\d\\.\\d\\.\\d.*"),
        is(true));
  }

}