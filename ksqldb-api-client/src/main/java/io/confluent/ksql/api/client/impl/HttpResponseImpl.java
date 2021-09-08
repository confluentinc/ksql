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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.Client.HttpResponse;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.Map;

public class HttpResponseImpl implements HttpResponse {

  private final int status;
  private final byte[] payload;

  public HttpResponseImpl(int status, byte[] payload) {
    this.status = status;
    this.payload = payload;
  }

  @Override
  public int status() {
    return status;
  }

  @Override
  public byte[] payloadAsBytes() {
    return payload;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Map<String, T> payloadAsMap() {
    if (payload == null) {
      return Collections.emptyMap();
    }
    JsonObject object = new JsonObject(Buffer.buffer(payload));
    return (Map<String, T>) object.getMap();
  }

}
