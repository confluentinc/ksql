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

package io.confluent.ksql.api.server;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.KsqlRestException;

public final class OldApiExceptionMapper {

  private OldApiExceptionMapper() {
  }

  public static EndpointResponse mapException(final Throwable exception) {
    if (exception instanceof KsqlRestException) {
      final KsqlRestException restException = (KsqlRestException) exception;
      return restException.getResponse();
    }
    return EndpointResponse.create()
        .status(INTERNAL_SERVER_ERROR.code())
        .type("application/json")
        .entity(new KsqlErrorMessage(Errors.ERROR_CODE_SERVER_ERROR, exception))
        .build();
  }
}
