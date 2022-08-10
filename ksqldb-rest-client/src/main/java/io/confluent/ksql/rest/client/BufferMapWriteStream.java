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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import java.util.function.Function;

@SuppressFBWarnings(value = "EI_EXPOSE_REP2")
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
  public Future<Void> write(final Buffer data) {
    return delegate.write(mapper.apply(data));
  }

  @Override
  public void write(final Buffer data, final Handler<AsyncResult<Void>> handler) {
    delegate.write(mapper.apply(data), handler);
  }

  @Override
  public Future<Void> end() {
    return delegate.end();
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
