/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import javax.ws.rs.core.Response;

/*
 * "Exception" used to conveniently return an error response from an
 * internal function call of a resource
 */
public class KsqlRestException extends RuntimeException {
  private final Response response;

  public KsqlRestException(final Response response) {
    this.response = response;
  }

  public Response getResponse() {
    return response;
  }
}
