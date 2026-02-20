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

package io.confluent.ksql.rest.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpConnection;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public class StreamPublisherTest {

  /**
   * Verifies that when SSL is enabled, StreamPublisher wraps the response
   * in a BufferCopyStream to prevent IndexOutOfBoundsException from Netty's
   * UnpooledSlicedByteBuf with fixed capacity.
   */
  @Test
  public void shouldWrapResponseInBufferCopyStreamWhenSslEnabled() {
    try (MockedStatic<VertxUtils> vertxUtils = mockStatic(VertxUtils.class);
         MockedConstruction<BufferCopyStream> mocked = mockConstruction(
             BufferCopyStream.class, (bcs, ctx) -> {
               when(bcs.handler(any())).thenReturn(bcs);
               when(bcs.exceptionHandler(any())).thenReturn(bcs);
               when(bcs.endHandler(any())).thenReturn(bcs);
               when(bcs.pause()).thenReturn(bcs);
               when(bcs.resume()).thenReturn(bcs);
               when(bcs.fetch(any(Long.class))).thenReturn(bcs);
             })) {

      vertxUtils.when(() -> VertxUtils.checkContext(any())).thenAnswer(inv -> null);

      final HttpClientResponse response = createMockResponse(true);

      // When:
      new StreamPublisher<>(
          mock(Context.class),
          response,
          buff -> buff,
          new CompletableFuture<>(),
          false
      );

      // Then:
      assertThat("BufferCopyStream should be constructed when SSL is enabled",
          mocked.constructed().size(), is(1));
    }
  }

  /**
   * Verifies that when SSL is disabled, StreamPublisher uses the response
   * directly without wrapping in BufferCopyStream.
   */
  @Test
  public void shouldNotWrapResponseInBufferCopyStreamWhenSslDisabled() {
    try (MockedStatic<VertxUtils> vertxUtils = mockStatic(VertxUtils.class);
         MockedConstruction<BufferCopyStream> mocked = mockConstruction(
             BufferCopyStream.class)) {

      vertxUtils.when(() -> VertxUtils.checkContext(any())).thenAnswer(inv -> null);

      final HttpClientResponse response = createMockResponse(false);

      // When:
      new StreamPublisher<>(
          mock(Context.class),
          response,
          buff -> buff,
          new CompletableFuture<>(),
          false
      );

      // Then:
      assertThat("BufferCopyStream should NOT be constructed when SSL is disabled",
          mocked.constructed().size(), is(0));
    }
  }

  private static HttpClientResponse createMockResponse(final boolean ssl) {
    final HttpClientResponse response = mock(HttpClientResponse.class);
    final HttpClientRequest request = mock(HttpClientRequest.class);
    final HttpConnection connection = mock(HttpConnection.class);

    when(response.request()).thenReturn(request);
    when(request.connection()).thenReturn(connection);
    when(connection.isSsl()).thenReturn(ssl);
    when(connection.closeHandler(any())).thenReturn(connection);

    when(response.handler(any())).thenReturn(response);
    when(response.exceptionHandler(any())).thenReturn(response);
    when(response.endHandler(any())).thenReturn(response);
    when(response.pause()).thenReturn(response);
    when(response.resume()).thenReturn(response);
    when(response.fetch(any(Long.class))).thenReturn(response);

    return response;
  }
}
