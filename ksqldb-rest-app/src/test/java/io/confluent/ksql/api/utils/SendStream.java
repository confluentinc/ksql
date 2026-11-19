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

package io.confluent.ksql.api.utils;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import java.util.LinkedList;
import java.util.Queue;

public class SendStream implements ReadStream<Buffer> {

  private final Context context;
  private final Queue<Buffer> pending = new LinkedList<>();
  private Handler<Buffer> handler;
  private Handler<Void> endHandler;
  private boolean ended;
  private boolean endSent;
  private long lastSentTime;

  public SendStream(final Vertx vertx) {
    this.context = vertx.getOrCreateContext();
  }

  public synchronized void acceptBuffer(final Buffer buffer) {
    pending.add(buffer);
    scheduleDrain();
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
    return this;
  }

  @Override
  public synchronized ReadStream<Buffer> handler(@Nullable final Handler<Buffer> handler) {
    this.handler = handler;
    scheduleDrain();
    return this;
  }

  public synchronized boolean isAttached() {
    return handler != null;
  }

  @Override
  public ReadStream<Buffer> pause() {
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    scheduleDrain();
    return this;
  }

  @Override
  public ReadStream<Buffer> fetch(final long amount) {
    return this;
  }

  @Override
  public synchronized ReadStream<Buffer> endHandler(@Nullable final Handler<Void> endHandler) {
    this.endHandler = endHandler;
    scheduleDrain();
    return this;
  }

  public synchronized void end() {
    this.ended = true;
    scheduleDrain();
  }

  public synchronized long getLastSentTime() {
    return lastSentTime;
  }

  private void scheduleDrain() {
    context.runOnContext(v -> drain());
  }

  // Only signal end after all pending buffers have been delivered to the handler.
  private synchronized void drain() {
    if (handler == null) {
      return;
    }
    Buffer buff;
    while ((buff = pending.poll()) != null) {
      lastSentTime = System.currentTimeMillis();
      handler.handle(buff);
    }
    if (ended && !endSent && endHandler != null) {
      endSent = true;
      endHandler.handle(null);
    }
  }
}
