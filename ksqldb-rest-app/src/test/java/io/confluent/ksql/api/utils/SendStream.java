/*
 * Copyright 2021 Confluent Inc.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
  private long lastSentTime;

  public SendStream(final Vertx vertx) {
    this.context = vertx.getOrCreateContext();
  }

  public synchronized void acceptBuffer(final Buffer buffer) {
    if (handler == null) {
      pending.add(buffer);
    } else {
      context.runOnContext(v -> sendBuffer(buffer));
    }
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
    return this;
  }

  @Override
  public synchronized ReadStream<Buffer> handler(@Nullable final Handler<Buffer> handler) {
    this.handler = handler;
    return this;
  }

  private synchronized void sendBuffer(final Buffer buff) {
    lastSentTime = System.currentTimeMillis();
    handler.handle(buff);
  }

  @Override
  public ReadStream<Buffer> pause() {
    return this;
  }

  @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
  @Override
  public ReadStream<Buffer> resume() {
    context.runOnContext(v -> {
      if (handler != null) {
        Buffer buff;
        while ((buff = pending.poll()) != null) {
          sendBuffer(buff);
        }
      }
    });

    return this;
  }

  @Override
  public ReadStream<Buffer> fetch(final long amount) {
    return this;
  }

  @Override
  public synchronized ReadStream<Buffer> endHandler(@Nullable final Handler<Void> endHandler) {
    this.endHandler = endHandler;
    if (ended && endHandler != null) {
      context.runOnContext(v -> endHandler.handle(null));
    }
    return this;
  }

  public synchronized void end() {
    this.ended = true;
    if (endHandler != null) {
      context.runOnContext(v -> endHandler.handle(null));
    }
  }

  public synchronized long getLastSentTime() {
    return lastSentTime;
  }
}


