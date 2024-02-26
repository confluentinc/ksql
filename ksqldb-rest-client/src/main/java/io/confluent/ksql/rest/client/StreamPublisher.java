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

import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.parsetools.RecordParser;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class StreamPublisher<T> extends BufferedPublisher<T> {

  private final HttpClientResponse response;
  private boolean drainHandlerSet;

  public static Buffer toJsonMsg(final Buffer responseLine, final boolean stripArray) {

    int start = 0;
    int end = responseLine.length() - 1;
    if (stripArray) {
      if (responseLine.getByte(0) == (byte) '[') {
        start = 1;
      }
      if (responseLine.getByte(end) == (byte) ']') {
        end -= 1;
      }
    }
    if (end > 0 && responseLine.getByte(end) == (byte) ',') {
      end -= 1;
    }
    return responseLine.slice(start, end + 1);
  }

  StreamPublisher(final Context context, final HttpClientResponse response,
      final Function<Buffer, T> mapper,
      final CompletableFuture<ResponseWithBody> bodyFuture,
      final boolean stripArray) {
    super(context);
    this.response = response;
    final RecordParser recordParser = RecordParser.newDelimited("\n", response);
    recordParser.exceptionHandler(bodyFuture::completeExceptionally)
        .handler(buff -> {
          if (buff.length() == 0) {
            // Ignore empty buffer - the server can insert random newlines!
            return;
          }
          final Buffer jsonMsg = toJsonMsg(buff, stripArray);
          if (!accept(mapper.apply(jsonMsg))) {
            if (!drainHandlerSet) {
              recordParser.pause();
              drainHandlerSet = true;
              drainHandler(() -> {
                drainHandlerSet = false;
                recordParser.resume();
              });
            }
          }
        })
        .endHandler(v -> complete());
    response.request().connection().closeHandler(v ->  complete());
  }

  public void close() {
    response.request().connection().close();
  }
}
