/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import java.util.function.Function;

public class BufferMapWriteStream<T> implements WriteStream<Buffer> {

  private final Function<Buffer, T> mapper;
  private final WriteStream<T> delegate;

  public BufferMapWriteStream(final Function<Buffer, T> mapper, final WriteStream<T> delegate) {
    this.mapper = mapper;
    this.delegate = delegate;
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
    delegate.exceptionHandler(handler);
    return this;
  }

  @Override
  public WriteStream<Buffer> write(final Buffer data) {
    delegate.write(mapper.apply(data));
    return this;
  }

  @Override
  public WriteStream<Buffer> write(final Buffer data, final Handler<AsyncResult<Void>> handler) {
    delegate.write(mapper.apply(data), handler);
    return this;
  }

  @Override
  public void end() {
    delegate.end();
  }

  @Override
  public void end(final Handler<AsyncResult<Void>> handler) {
    delegate.end(handler);
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(final int maxSize) {
    delegate.setWriteQueueMaxSize(maxSize);
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return delegate.writeQueueFull();
  }

  @Override
  public WriteStream<Buffer> drainHandler(@Nullable final Handler<Void> handler) {
    delegate.drainHandler(handler);
    return this;
  }
}
