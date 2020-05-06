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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.io.OutputStream;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/*
An OutputStream that writes to a HttpServerResponse.
<p>
This is only used by legacy streaming endpoints from the old API which work with output streams.
 */
public class ResponseOutputStream extends OutputStream {

  private final HttpServerResponse response;

  public ResponseOutputStream(final HttpServerResponse response) {
    this.response = response;
  }

  @Override
  public void write(final int b) {
    // We never want to write single bytes - it's inefficient
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(final @NotNull byte[] bytes, final int offset, final int length) {
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
    response.write(buffer);
  }

  @Override
  public void close() {
    response.end();
  }
}


