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

import io.confluent.ksql.api.client.Client.HttpRequest;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class HttpRequestImpl implements HttpRequest {

  private final Map<String, Object> payloadAsMap = new HashMap<>();
  private String path;
  private String method;

  @Override
  public String path() {
    return this.path;
  }

  @Override
  public String method() {
    return this.method;
  }

  @Override
  public Map<String, Object> payload() {
    return new HashMap<>(payloadAsMap);
  }

  @Override
  public HttpRequest path(String path) {
    Objects.requireNonNull(path, "path may not be null");
    this.path = path;
    return this;
  }

  @Override
  public HttpRequest payload(Map<String, Object> payload) {
    Objects.requireNonNull(payload, "payload may not be null");
    payloadAsMap.putAll(Objects.requireNonNull(payload));
    return this;
  }

  @Override
  public HttpRequest payload(String key, Object value) {
    Objects.requireNonNull(key, "key may not be null");
    Objects.requireNonNull(value, "value may not be null");
    payloadAsMap.put(key, value);
    return this;
  }

  @Override
  public HttpRequest method(String method) {
    Objects.requireNonNull(method, "HTTP method may not be null");
    this.method = method;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpRequestImpl that = (HttpRequestImpl) o;

    return Objects.equals(payloadAsMap, that.payloadAsMap)
        && Objects.equals(path, that.path)
        && (Objects.equals(method, that.method) || (method != null && method.equalsIgnoreCase(that.method)));
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        payloadAsMap,
        path,
        method != null ? method.toUpperCase(Locale.ROOT) : null
    );
  }
}
