/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import java.util.Optional;
import java.util.function.Function;
import javax.naming.AuthenticationException;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;

public final class KsqlClientUtil {

  private KsqlClientUtil() {
  }

  public static <T> RestResponse<T> toRestResponse(
      final Response response,
      final String path,
      final Function<Response, T> mapper
  ) {
    final Code statusCode = HttpStatus.getCode(response.getStatus());
    return statusCode == Code.OK
        ? RestResponse.successful(statusCode, mapper.apply(response))
        : createErrorResponse(path, response);
  }

  private static <T> RestResponse<T> createErrorResponse(
      final String path,
      final Response response
  ) {
    final Code statusCode = HttpStatus.getCode(response.getStatus());
    final Optional<KsqlErrorMessage> errorMessage = tryReadErrorMessage(response);
    if (errorMessage.isPresent()) {
      return RestResponse.erroneous(statusCode, errorMessage.get());
    }

    if (statusCode == Code.NOT_FOUND) {
      return RestResponse.erroneous(statusCode,
          "Path not found. Path='" + path + "'. "
              + "Check your ksql http url to make sure you are connecting to a ksql server."
      );
    }

    if (statusCode == Code.UNAUTHORIZED) {
      return RestResponse.erroneous(statusCode, unauthorizedErrorMsg());
    }

    if (statusCode == Code.FORBIDDEN) {
      return RestResponse.erroneous(statusCode, forbiddenErrorMsg());
    }

    return RestResponse.erroneous(
        statusCode,
        "The server returned an unexpected error: "
            + response.getStatusInfo().getReasonPhrase());
  }

  private static Optional<KsqlErrorMessage> tryReadErrorMessage(final Response response) {
    try {
      return Optional.ofNullable(response.readEntity(KsqlErrorMessage.class));
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  private static KsqlErrorMessage unauthorizedErrorMsg() {
    return new KsqlErrorMessage(
        Errors.ERROR_CODE_UNAUTHORIZED,
        new AuthenticationException(
            "Could not authenticate successfully with the supplied credentials.")
    );
  }

  private static KsqlErrorMessage forbiddenErrorMsg() {
    return new KsqlErrorMessage(
        Errors.ERROR_CODE_FORBIDDEN,
        new AuthenticationException("You are forbidden from using this cluster.")
    );
  }
}
