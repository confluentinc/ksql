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

package io.confluent.ksql.rest;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.Map;

public final class EndpointResponse {

  private final Map<String, String> headers;
  private final int status;
  private final Object entity;

  private EndpointResponse(final Map<String, String> headers, final int status,
      final Object entity) {
    this.headers = headers;
    this.status = status;
    this.entity = entity;
  }

  public int getStatus() {
    return status;
  }

  public Object getEntity() {
    return entity;
  }

  public static Builder create() {
    return new Builder();
  }

  public static EndpointResponse ok(final Object entity) {
    return new EndpointResponse(null, HttpResponseStatus.OK.code(), entity);
  }

  public static EndpointResponse failed(final int status) {
    return new EndpointResponse(null, status, null);
  }

  public static final class Builder {

    private int status;
    private Map<String, String> headers;
    private Object entity;

    private Builder() {
    }

    public Builder status(final int status) {
      this.status = status;
      return this;
    }

    public Builder header(final String key, final Object value) {
      if (headers == null) {
        headers = new HashMap<>();
      }
      headers.put(key, value.toString());
      return this;
    }

    public Builder entity(final Object entity) {
      this.entity = entity;
      return this;
    }

    public Builder type(final String contentType) {
      return header("content-type", contentType);
    }

    public EndpointResponse build() {
      return new EndpointResponse(headers, status, entity);
    }

  }

}