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

package io.confluent.ksql.rest.client;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import java.util.Objects;

class ResponseWithBody {

  private final HttpClientResponse response;
  private final Buffer body;

  ResponseWithBody(final HttpClientResponse response, final Buffer body) {
    this.response = Objects.requireNonNull(response);
    this.body = Objects.requireNonNull(body);
  }

  ResponseWithBody(final HttpClientResponse response) {
    this.response = Objects.requireNonNull(response);
    this.body = null;
  }

  public HttpClientResponse getResponse() {
    return response;
  }

  public Buffer getBody() {
    return body;
  }
}
