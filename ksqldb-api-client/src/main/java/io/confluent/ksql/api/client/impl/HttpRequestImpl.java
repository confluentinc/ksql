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

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Client.HttpRequest;
import io.confluent.ksql.api.client.Client.HttpResponse;
import io.vertx.core.http.HttpMethod;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class HttpRequestImpl implements HttpRequest {

  private final Map<String, Object> payloadAsMap = new HashMap<>();
  private final Map<String, Object> properties = new HashMap<>();
  private final String path;
  private final HttpMethod method;
  private final Client client;
  private String propertiesKey = "properties";

  public HttpRequestImpl(final String method, final String path, final Client client) {
    this.path = Objects.requireNonNull(path, "Path may not be null");
    this.method = HttpMethod.valueOf(
        Objects.requireNonNull(method, "HTTP method may not be null")
    );
    this.client = Objects.requireNonNull(client, "Client may not be null");
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
  public HttpRequest payload(final Map<String, Object> payload) {
    Objects.requireNonNull(payload, "Payload may not be null");
    payload.forEach(this::payload);
    return this;
  }

  @Override
  public HttpRequest payload(final String key, final Object value) {
    Objects.requireNonNull(key, "Payload key may not be null");
    Objects.requireNonNull(value, "Payload value may not be null");
    payloadAsMap.put(key, value);
    return this;
  }

  @Override
  public Map<String, Object> properties() {
    return new HashMap<>(properties);
  }

  @Override
  public HttpRequest properties(final Map<String, Object> properties) {
    Objects.requireNonNull(properties, "Properties map may not be null");
    properties.forEach(this::property);
    return this;
  }

  @Override
  public HttpRequest property(final String key, final Object value) {
    Objects.requireNonNull(key, "Property key may not be null");
    Objects.requireNonNull(value, "Property value may not be null");

    properties.put(key, value);
    return this;
  }

  @Override
  public HttpRequest propertiesKey(final String propertiesKey) {
    this.propertiesKey = propertiesKey;
    return this;
  }

  public String propertiesKey() {
    return propertiesKey;
  }

  Map<String, Object> buildPayload() {
    final Map<String, Object> payload = payload();
    // include properties
    payload.put(propertiesKey(), properties());
    return payload;
  }

  @Override
  public CompletableFuture<HttpResponse> send() {
    return ((ClientImpl) client).send(method, path, buildPayload());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HttpRequestImpl that = (HttpRequestImpl) o;

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
