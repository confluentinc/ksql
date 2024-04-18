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

import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.vertx.core.buffer.Buffer;
import java.util.Optional;
import java.util.function.Function;
import javax.naming.AuthenticationException;

public final class KsqlClientUtil {

  private KsqlClientUtil() {
  }

  public static <T> RestResponse<T> toRestResponse(
      final ResponseWithBody resp,
      final String path,
      final Function<ResponseWithBody, T> mapper
  ) {
    final int statusCode = resp.getResponse().statusCode();
    return statusCode == OK.code()
        ? RestResponse.successful(statusCode, mapper.apply(resp))
        : createErrorResponse(path, resp);
  }

  static <T> T deserialize(final Buffer buffer, final Class<T> clazz) {
    final ObjectMapper objectMapper = ApiJsonMapper.INSTANCE.get();
    try {
      return objectMapper.readValue(buffer.getBytes(), clazz);
    } catch (Exception e) {
      throw new KsqlRestClientException("Failed to deserialise object", e);
    }
  }

  static Buffer serialize(final Object object) {
    final ObjectMapper objectMapper = ApiJsonMapper.INSTANCE.get();
    try {
      final byte[] bytes = objectMapper.writeValueAsBytes(object);
      return Buffer.buffer(bytes);
    } catch (Exception e) {
      throw new KsqlRestClientException("Failed to serialise object", e);
    }
  }

  private static <T> RestResponse<T> createErrorResponse(
      final String path,
      final ResponseWithBody resp
  ) {
    final int statusCode = resp.getResponse().statusCode();
    final Optional<KsqlErrorMessage> errorMessage = tryReadErrorMessage(resp);
    if (errorMessage.isPresent()) {
      return RestResponse.erroneous(statusCode, errorMessage.get());
    }

    if (statusCode == NOT_FOUND.code()) {
      return RestResponse.erroneous(statusCode,
          "Path not found. Path='" + path + "'. "
              + "Check your ksql http url to make sure you are connecting to a ksql server."
      );
    }

    if (statusCode == UNAUTHORIZED.code()) {
      return RestResponse.erroneous(statusCode, unauthorizedErrorMsg());
    }

    if (statusCode == FORBIDDEN.code()) {
      return RestResponse.erroneous(statusCode, forbiddenErrorMsg());
    }

    return RestResponse.erroneous(
        statusCode,
        "The server returned an unexpected error: "
            + resp.getResponse().statusMessage());
  }

  private static Optional<KsqlErrorMessage> tryReadErrorMessage(
      final ResponseWithBody resp) {
    try {
      return Optional.ofNullable(deserialize(resp.getBody(), KsqlErrorMessage.class));
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
