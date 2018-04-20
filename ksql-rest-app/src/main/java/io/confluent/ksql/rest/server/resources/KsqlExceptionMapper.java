/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.entity.KsqlErrorMessage;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class KsqlExceptionMapper implements ExceptionMapper<Throwable> {

  @Override
  public Response toResponse(Throwable exception) {
    // TODO: Distinguish between exceptions that warrant a stack trace and ones that don't
    if (exception instanceof KsqlRestException) {
      KsqlRestException restException = (KsqlRestException)exception;
      return restException.getResponse();
    }
    if (exception instanceof WebApplicationException) {
      WebApplicationException webApplicationException = (WebApplicationException)exception;
      return Response
          .status(
              Response.Status.fromStatusCode(
                  webApplicationException.getResponse().getStatus()))
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(
              new KsqlErrorMessage(
                  webApplicationException.getResponse().getStatus()
                      * Errors.HTTP_TO_ERROR_CODE_MULTIPLIER,
                  webApplicationException))
          .build();
    }
    return Response
        .status(Response.Status.INTERNAL_SERVER_ERROR)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(new KsqlErrorMessage(Errors.ERROR_CODE_SERVER_ERROR, exception))
        .build();
  }
}
