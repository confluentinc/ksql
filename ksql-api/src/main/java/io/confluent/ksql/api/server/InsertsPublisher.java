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

package io.confluent.ksql.api.server;

import io.vertx.core.Context;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;

public class InsertsPublisher extends BufferedPublisher<JsonObject> {

  private final RecordParser recordParser;

  public InsertsPublisher(final Context ctx, final RecordParser recordparser) {
    super(ctx);
    this.recordParser = recordparser;
  }

  public void handleBuffer(final Buffer buffer) {
    final JsonObject row = new JsonObject(buffer);
    accept(row);
    if (numBuffered() != 0) {
      recordParser.pause();
      drainHandler(this::drained);
    }
  }

  private void drained() {
    recordParser.resume();
  }
}
