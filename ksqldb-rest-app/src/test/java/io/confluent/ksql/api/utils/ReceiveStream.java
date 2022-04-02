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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

public class ReceiveStream implements WriteStream<Buffer> {

  private final Vertx vertx;
  private final Buffer body = Buffer.buffer();
  private long firstReceivedTime;
  private boolean ended;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public ReceiveStream(final Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public WriteStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
    return this;
  }

  @Override
  public synchronized Future<Void> write(final Buffer data) {
    firstReceivedTime = System.currentTimeMillis();
    body.appendBuffer(data);
    return Future.succeededFuture();
  }

  @Override
  public void write(final Buffer data, final Handler<AsyncResult<Void>> handler) {
    body.appendBuffer(data);
    if (handler != null) {
      vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
    }
  }

  @Override
  public synchronized Future<Void> end() {
    ended = true;
    return Future.succeededFuture();
  }

  @Override
  public void end(final Handler<AsyncResult<Void>> handler) {
    end();
    if (handler != null) {
      vertx.runOnContext(v -> handler.handle(Future.succeededFuture()));
    }
  }

  @Override
  public WriteStream<Buffer> setWriteQueueMaxSize(final int maxSize) {
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return false;
  }

  @Override
  public WriteStream<Buffer> drainHandler(@Nullable final Handler<Void> handler) {
    return this;
  }

  public synchronized Buffer getBody() {
    return body;
  }

  public synchronized long getFirstReceivedTime() {
    return firstReceivedTime;
  }

  public synchronized boolean isEnded() {
    return ended;
  }
}
