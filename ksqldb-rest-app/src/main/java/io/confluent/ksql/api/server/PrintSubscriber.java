/*
 * Copyright 2022 Confluent Inc.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.vertx.core.Context;
import io.vertx.core.http.HttpServerResponse;
import java.util.Objects;
import org.reactivestreams.Subscription;

/**
 * This is a reactive streams subscriber which receives a stream of results from the
 * BlockingPrintPublisher and writes the output to the HTTP2 response in delimited format using new
 * line.
 *
 * <p>The response comprises a sequence of topic records, separated by newline. The overall
 * response does not form a single JSON object or array. This makes it easier to parse at the client
 * without recourse to a streaming JSON parser.
 *
 * <p>Each entry in the stream is a string with fixed fields.
 *
 * <p>Please consult the API
 * documentation (https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/print/) for a
 * description on how formatting works. The format used
 * here is exactly the same used by the previous Websocket implementation of the print topic
 * functionality.
 */
public class PrintSubscriber extends BaseSubscriber<String> {

  private static final int REQUEST_BATCH_SIZE = 200;

  private final HttpServerResponse response;
  private int tokens;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PrintSubscriber(final Context context, final HttpServerResponse response) {
    super(context);
    this.response = Objects.requireNonNull(response);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkMakeRequest();
  }

  @Override
  public void handleValue(final String row) {
    response.write(row + "\n");
    tokens--;
    if (response.writeQueueFull()) {
      response.drainHandler(v -> checkMakeRequest());
    } else {
      checkMakeRequest();
    }
  }

  private void checkMakeRequest() {
    if (tokens == 0) {
      tokens = REQUEST_BATCH_SIZE;
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  @Override
  public void handleComplete() {
    response.end();
  }

}