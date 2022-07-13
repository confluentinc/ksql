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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.Client.HttpResponse;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class HttpResponseImpl implements HttpResponse {

  private final int status;
  private final byte[] body;

  public HttpResponseImpl(final int status, final byte[] body) {
    this.status = status;
    if (body == null) {
      this.body = null;
    } else {
      this.body = new byte[body.length];
      System.arraycopy(body, 0, this.body, 0, body.length);
    }
  }

  @Override
  public int status() {
    return status;
  }

  @Override
  public byte[] body() {
    if (body == null) {
      return null;
    } else {
      final byte[] copy = new byte[body.length];
      System.arraycopy(body, 0, copy, 0, body.length);
      return copy;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Map<String, T> bodyAsMap() {
    if (body == null) {
      return Collections.emptyMap();
    }
    try {
      final JsonObject object = new JsonObject(Buffer.buffer(body));
      return (Map<String, T>) object.getMap();
    } catch (DecodeException e) {
      throw new KsqlClientException(
          "Could not decode response: " + new String(body, StandardCharsets.UTF_8)
      );
    }
  }
}
