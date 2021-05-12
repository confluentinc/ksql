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

package io.confluent.ksql.api.server;

import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

/*
An OutputStream that writes to a HttpServerResponse.
<p>
This is only used by legacy streaming endpoints from the old API which work with output streams.
 */
public class ResponseOutputStream extends OutputStream {
  private static final int BLOCK_TIME_MS = 100;

  private final HttpServerResponse response;
  private final int writeTimeoutMs;
  private volatile boolean closed;

  public ResponseOutputStream(final HttpServerResponse response, final int writeTimeoutMs) {
    this.response = response;
    this.writeTimeoutMs = writeTimeoutMs;
  }

  @Override
  public void write(final int b) {
    // We never want to write single bytes - it's inefficient
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void write(final @NotNull byte[] bytes, final int offset, final int length)
      throws IOException {
    if (closed) {
      throw new EOFException("ResponseOutputStream is closed");
    }
    Objects.requireNonNull(bytes);
    if ((offset < 0) || (offset > bytes.length)) {
      throw new IndexOutOfBoundsException();
    }
    if ((length < 0) || ((offset + length) > bytes.length) || ((offset + length) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (length == 0) {
      return;
    }
    final byte[] bytesToWrite = new byte[length];
    System.arraycopy(bytes, offset, bytesToWrite, 0, length);
    final Buffer buffer = Buffer.buffer(bytesToWrite);
    blockIfWriteQueueFull();
    response.write(buffer);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    response.end();
  }

  private void blockIfWriteQueueFull() throws IOException {
    VertxUtils.checkIsWorker();
    if (response.writeQueueFull()) {
      final CompletableFuture<Void> cf = new CompletableFuture<>();
      response.drainHandler(v -> cf.complete(null));
      blockOnWrite(cf);
    }
  }

  private void blockOnWrite(final CompletableFuture<Void> cf) throws IOException {
    final long start = System.currentTimeMillis();
    do {
      if (closed) {
        throw new EOFException("ResponseOutputStream is closed");
      }
      try {
        // We block for a small amount of time so we can check if the stream has been closed
        cf.get(BLOCK_TIME_MS, TimeUnit.MILLISECONDS);
        return;
      } catch (TimeoutException e) {
        // continue loop
      } catch (Exception e) {
        throw new KsqlException(e);
      }
    } while (System.currentTimeMillis() - start < writeTimeoutMs);
    throw new KsqlException("Timed out waiting to write to client");
  }

}


