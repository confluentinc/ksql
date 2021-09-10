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
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.Map;

public class HttpResponseImpl implements HttpResponse {

  private final int status;
  private final byte[] payloadBytes;

  public HttpResponseImpl(int status, byte[] payloadBytes) {
    this.status = status;
    this.payloadBytes = payloadBytes;
  }

  @Override
  public int status() {
    return status;
  }

  @Override
  public byte[] payloadAsBytes() {
    return payloadBytes;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Map<String, T> payloadAsMap() {
    if (payloadBytes == null) {
      return Collections.emptyMap();
    }
    try {
      JsonObject object = new JsonObject(Buffer.buffer(payloadBytes));
      return (Map<String, T>) object.getMap();
    } catch (DecodeException e) {
      throw new KsqlClientException("could not decode response: " + new String(payloadBytes));
    }
  }

}
