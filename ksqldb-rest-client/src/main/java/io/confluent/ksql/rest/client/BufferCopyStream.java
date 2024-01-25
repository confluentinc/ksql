/*
 * Copyright 2019 Confluent Inc.
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

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import java.util.Objects;

public final class BufferCopyStream implements ReadStream<Buffer> {

  private final ReadStream<Buffer> source;

  public BufferCopyStream(final ReadStream<Buffer> source) {
    Objects.requireNonNull(source, "Source must be non-null");
    this.source = source;
  }

  @Override
  public ReadStream<Buffer> handler(final Handler<Buffer> handler) {
    if (handler == null) {
      source.handler(null);
    } else {
      source.handler(event -> handler.handle(event.copy()));
    }
    return this;
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
    source.exceptionHandler(handler);
    return this;
  }

  @Override
  public ReadStream<Buffer> pause() {
    source.pause();
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    source.resume();
    return this;
  }

  @Override
  public ReadStream<Buffer> fetch(final long amount) {
    source.fetch(amount);
    return this;
  }

  @Override
  public ReadStream<Buffer> endHandler(final Handler<Void> endHandler) {
    source.endHandler(endHandler);
    return this;
  }
}