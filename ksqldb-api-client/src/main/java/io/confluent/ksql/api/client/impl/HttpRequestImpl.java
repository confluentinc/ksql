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

import io.confluent.ksql.api.client.Client.HttpRequest;
import io.confluent.ksql.api.client.Client.HttpResponse;
import io.vertx.core.http.HttpMethod;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class HttpRequestImpl implements HttpRequest {

  private final Map<String, Object> payloadAsMap = new HashMap<>();
  private final Map<String, Object> properties = new HashMap<>();
  private final String path;
  private final HttpMethod method;
  private final ClientImpl client;

  public HttpRequestImpl(String method, String path) {
    this(method, path, null);
  }

  public HttpRequestImpl(String method, String path, ClientImpl client) {
    Objects.requireNonNull(path, "Path may not be null");
    Objects.requireNonNull(method, "HTTP method may not be null");
    this.path = path;
    this.method = HttpMethod.valueOf(method.toUpperCase(Locale.ROOT));
    this.client = client;
  }

  @Override
  public String path() {
    return this.path;
  }

  @Override
  public String method() {
    return this.method.name();
  }

  @Override
  public Map<String, Object> payload() {
    return new HashMap<>(payloadAsMap);
  }

  @Override
  public HttpRequest payload(Map<String, Object> payload) {
    Objects.requireNonNull(payload, "Payload may not be null");
    payloadAsMap.putAll(Objects.requireNonNull(payload));
    return this;
  }

  @Override
  public Map<String, Object> properties() {
    return new HashMap<>(properties);
  }

  @Override
  public HttpRequest payload(String key, Object value) {
    Objects.requireNonNull(key, "Payload key may not be null");
    Objects.requireNonNull(value, "Payload value may not be null");
    payloadAsMap.put(key, value);
    return this;
  }

  @Override
  public HttpRequest property(String key, Object value) {
    Objects.requireNonNull(key, "Property key may not be null");
    Objects.requireNonNull(value, "Property value may not be null");

    properties.put(key, value);
    return this;
  }

  @Override
  public HttpRequest properties(Map<String, Object> properties) {
    Objects.requireNonNull(properties, "Properties map may not be null");
    this.properties.putAll(properties);
    return this;
  }

  @Override
  public CompletableFuture<HttpResponse> send() {
    return client.send(this);
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
        && Objects.equals(properties, that.properties)
        && Objects.equals(path, that.path)
        && Objects.equals(method, that.method);
  }

  @Override
  public int hashCode() {
    return Objects.hash(payloadAsMap, path, method, properties);
  }
}
