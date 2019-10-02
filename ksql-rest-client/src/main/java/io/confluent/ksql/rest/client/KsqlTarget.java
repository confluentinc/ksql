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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerInfo;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.naming.AuthenticationException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.glassfish.jersey.client.ClientProperties;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlTarget {

  private static final int MAX_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(32);

  private static final String STATUS_PATH = "/status";
  private static final String KSQL_PATH = "/ksql";
  private static final String QUERY_PATH = "/query";

  private final WebTarget target;
  private final LocalProperties localProperties;
  private final Optional<String> authHeader;

  KsqlTarget(
      final WebTarget target,
      final LocalProperties localProperties,
      final Optional<String> authHeader
  ) {
    this.target = requireNonNull(target, "target");
    this.localProperties = requireNonNull(localProperties, "localProperties");
    this.authHeader = requireNonNull(authHeader, "authHeader");
  }

  public KsqlTarget authorizationHeader(final String authHeader) {
    return new KsqlTarget(target, localProperties, Optional.of(authHeader));
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return get("/info", ServerInfo.class);
  }

  public RestResponse<CommandStatuses> getStatuses() {
    return get(STATUS_PATH, CommandStatuses.class);
  }

  public RestResponse<CommandStatus> getStatus(final String commandId) {
    return get(STATUS_PATH + "/" + commandId, CommandStatus.class);
  }

  public RestResponse<KsqlEntityList> postKsqlRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        KSQL_PATH,
        ksqlRequest(ksql, previousCommandSeqNum),
        Optional.empty(),
        true,
        r -> r.readEntity(KsqlEntityList.class)
    );
  }

  public RestResponse<QueryStream> postQueryRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        QUERY_PATH,
        ksqlRequest(ksql, previousCommandSeqNum),
        Optional.of(QueryStream.READ_TIMEOUT_MS),
        false,
        QueryStream::new
    );
  }

  public RestResponse<InputStream> postPrintTopicRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        QUERY_PATH,
        ksqlRequest(ksql, previousCommandSeqNum),
        Optional.empty(),
        false,
        r -> (InputStream) r.getEntity()
    );
  }

  private KsqlRequest ksqlRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return new KsqlRequest(
        ksql,
        localProperties.toMap(),
        previousCommandSeqNum.orElse(null)
    );
  }

  private <T> RestResponse<T> get(final String path, final Class<T> type) {
    try (Response response = target
        .path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .headers(headers())
        .get()
    ) {
      final Code statusCode = HttpStatus.getCode(response.getStatus());
      return statusCode == Code.OK
          ? RestResponse.successful(statusCode, response.readEntity(type))
          : createErrorResponse(path, response);

    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing GET to KSQL server. path:" + path, e);
    }
  }

  private <T> RestResponse<T> post(
      final String path,
      final Object jsonEntity,
      final Optional<Integer> readTimeoutMs,
      final boolean closeResponse,
      final Function<Response, T> mapper
  ) {
    Response response = null;

    try {
      response = target
          .path(path)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .property(ClientProperties.READ_TIMEOUT, readTimeoutMs.orElse(0))
          .headers(headers())
          .post(Entity.json(jsonEntity));

      final Code statusCode = HttpStatus.getCode(response.getStatus());
      return statusCode == Code.OK
          ? RestResponse.successful(statusCode, mapper.apply(response))
          : createErrorResponse(path, response);

    } catch (final ProcessingException e) {
      if (shouldRetry(readTimeoutMs, e)) {
        return post(path, jsonEntity, calcReadTimeout(readTimeoutMs), closeResponse, mapper);
      }
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } finally {
      if (response != null && closeResponse) {
        response.close();
      }
    }
  }

  private MultivaluedMap<String, Object> headers() {
    final MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    authHeader.ifPresent(v -> headers.add(HttpHeaders.AUTHORIZATION, v));
    return headers;
  }

  private static boolean shouldRetry(
      final Optional<Integer> readTimeoutMs,
      final ProcessingException e
  ) {
    return readTimeoutMs.map(timeout -> timeout < MAX_TIMEOUT).orElse(false)
        && e.getCause() instanceof SocketTimeoutException;
  }

  private static Optional<Integer> calcReadTimeout(final Optional<Integer> previousTimeoutMs) {
    return previousTimeoutMs.map(timeout -> Math.min(timeout * 2, MAX_TIMEOUT));
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
