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

package io.confluent.ksql.api.spi;

import java.util.Objects;
import javax.ws.rs.core.Response;

public interface EndpointResponse {

  int getStatusCode();

  String getStatusMessage();

  Object getResponseBody();

  static EndpointResponse create(final Response response) {
    return create(response.getStatus(), response.getStatusInfo().getReasonPhrase(),
        response.getEntity());
  }

  static EndpointResponse create(final int statusCode, final String statusMessage,
      final Object responseBody) {
    Objects.requireNonNull(responseBody);
    return new EndpointResponse() {

      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getStatusMessage() {
        return statusMessage;
      }

      @Override
      public Object getResponseBody() {
        return responseBody;
      }
    };
  }
}