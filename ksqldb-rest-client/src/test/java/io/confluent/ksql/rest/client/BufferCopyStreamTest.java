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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BufferCopyStreamTest {

  @Mock
  private ReadStream<Buffer> source;
  @Mock
  private Handler<Buffer> bufferHandler;
  @Mock
  private Handler<Throwable> exceptionHandler;
  @Mock
  private Handler<Void> endHandler;

  @Captor
  private ArgumentCaptor<Handler<Buffer>> handlerCaptor;

  private BufferCopyStream stream;

  @Before
  public void setUp() {
    when(source.handler(any())).thenReturn(source);
    when(source.exceptionHandler(any())).thenReturn(source);
    when(source.pause()).thenReturn(source);
    when(source.resume()).thenReturn(source);
    when(source.fetch(any(Long.class))).thenReturn(source);
    when(source.endHandler(any())).thenReturn(source);
    stream = new BufferCopyStream(source);
  }

  @Test
  public void shouldThrowOnNullSource() {
    // When/Then:
    assertThrows(NullPointerException.class, () -> new BufferCopyStream(null));
  }

  @Test
  public void shouldCopyBufferOnHandler() {
    // Given:
    stream.handler(bufferHandler);
    verify(source).handler(handlerCaptor.capture());
    final Handler<Buffer> wrappedHandler = handlerCaptor.getValue();

    final byte[] data = "hello".getBytes();
    final Buffer original = Buffer.buffer(data);

    // When:
    wrappedHandler.handle(original);

    // Then:
    final ArgumentCaptor<Buffer> bufferCaptor = ArgumentCaptor.forClass(Buffer.class);
    verify(bufferHandler).handle(bufferCaptor.capture());
    final Buffer copied = bufferCaptor.getValue();
    assertThat(copied.getBytes(), is(original.getBytes()));
  }

  @Test
  public void shouldPassNullHandlerToSource() {
    // When:
    stream.handler(null);

    // Then:
    verify(source).handler(isNull());
  }

  @Test
  public void shouldDelegateExceptionHandler() {
    // When:
    final ReadStream<Buffer> result = stream.exceptionHandler(exceptionHandler);

    // Then:
    verify(source).exceptionHandler(exceptionHandler);
    assertThat(result, is(sameInstance(stream)));
  }

  @Test
  public void shouldDelegatePause() {
    // When:
    final ReadStream<Buffer> result = stream.pause();

    // Then:
    verify(source).pause();
    assertThat(result, is(sameInstance(stream)));
  }

  @Test
  public void shouldDelegateResume() {
    // When:
    final ReadStream<Buffer> result = stream.resume();

    // Then:
    verify(source).resume();
    assertThat(result, is(sameInstance(stream)));
  }

  @Test
  public void shouldDelegateFetch() {
    // When:
    final ReadStream<Buffer> result = stream.fetch(42);

    // Then:
    verify(source).fetch(42);
    assertThat(result, is(sameInstance(stream)));
  }

  @Test
  public void shouldDelegateEndHandler() {
    // When:
    final ReadStream<Buffer> result = stream.endHandler(endHandler);

    // Then:
    verify(source).endHandler(endHandler);
    assertThat(result, is(sameInstance(stream)));
  }
}
