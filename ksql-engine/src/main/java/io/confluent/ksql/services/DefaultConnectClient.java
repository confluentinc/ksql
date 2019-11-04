/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import javax.ws.rs.core.HttpHeaders;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation of {@code ConnectClient}. This implementation is
 * thread safe, and the methods are all <i>blocking</i> and are configured with
 * default timeouts of {@value #DEFAULT_TIMEOUT_MS}ms.
 */
public class DefaultConnectClient implements ConnectClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultConnectClient.class);
  private static final ObjectMapper MAPPER = JsonMapper.INSTANCE.mapper;

  private static final String CONNECTORS = "/connectors";
  private static final String STATUS = "/status";
  private static final int DEFAULT_TIMEOUT_MS = 5_000;
  private static final int MAX_ATTEMPTS = 3;

  private final URI connectUri;
  private final Optional<String> authHeader;

  public DefaultConnectClient(
      final String connectUri,
      final Optional<String> authHeader
  ) {
    Objects.requireNonNull(connectUri, "connectUri");
    this.authHeader = Objects.requireNonNull(authHeader, "authHeader");

    try {
      this.connectUri = new URI(connectUri);
    } catch (URISyntaxException e) {
      throw new KsqlException(
          "Could not initialize connect client due to invalid URI: " + connectUri, e);
    }
  }

  @Override
  public ConnectResponse<ConnectorInfo> create(
      final String connector,
      final Map<String, String> config
  ) {
    try {
      LOG.debug("Issuing create request to Kafka Connect at URI {} with name {} and config {}",
          connectUri,
          connector,
          config);

      final ConnectResponse<ConnectorInfo> connectResponse = withRetries(() -> Request
          .Post(connectUri.resolve(CONNECTORS))
          .setHeaders(headers())
          .socketTimeout(DEFAULT_TIMEOUT_MS)
          .connectTimeout(DEFAULT_TIMEOUT_MS)
          .bodyString(
              MAPPER.writeValueAsString(
                  ImmutableMap.of(
                      "name", connector,
                      "config", config)),
              ContentType.APPLICATION_JSON
          )
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_CREATED, ConnectorInfo.class, Function.identity())));

      connectResponse.error()
          .ifPresent(error -> LOG.warn("Did not CREATE connector {}: {}", connector, error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public ConnectResponse<List<String>> connectors() {
    try {
      LOG.debug("Issuing request to Kafka Connect at URI {} to list connectors", connectUri);

      final ConnectResponse<List<String>> connectResponse = withRetries(() -> Request
          .Get(connectUri.resolve(CONNECTORS))
          .setHeaders(headers())
          .socketTimeout(DEFAULT_TIMEOUT_MS)
          .connectTimeout(DEFAULT_TIMEOUT_MS)
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_OK, List.class, foo -> (List<String>) foo)));

      connectResponse.error()
          .ifPresent(error -> LOG.warn("Could not list connectors: {}.", error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  @Override
  public ConnectResponse<ConnectorStateInfo> status(final String connector) {
    try {
      LOG.debug("Issuing status request to Kafka Connect at URI {} with name {}",
          connectUri,
          connector);

      final ConnectResponse<ConnectorStateInfo> connectResponse = withRetries(() -> Request
          .Get(connectUri.resolve(CONNECTORS + "/" + connector + STATUS))
          .setHeaders(headers())
          .socketTimeout(DEFAULT_TIMEOUT_MS)
          .connectTimeout(DEFAULT_TIMEOUT_MS)
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_OK, ConnectorStateInfo.class, Function.identity())));

      connectResponse.error()
          .ifPresent(error ->
              LOG.warn("Could not query status of connector {}: {}", connector, error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  @Override
  public ConnectResponse<ConnectorInfo> describe(final String connector) {
    try {
      LOG.debug("Issuing request to Kafka Connect at URI {} to get config for {}",
          connectUri, connector);

      final ConnectResponse<ConnectorInfo> connectResponse = withRetries(() -> Request
          .Get(connectUri.resolve(String.format("%s/%s", CONNECTORS, connector)))
          .setHeaders(headers())
          .socketTimeout(DEFAULT_TIMEOUT_MS)
          .connectTimeout(DEFAULT_TIMEOUT_MS)
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_OK, ConnectorInfo.class, Function.identity())));

      connectResponse.error()
          .ifPresent(error -> LOG.warn("Could not list connectors: {}.", error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  @Override
  public ConnectResponse<String> delete(final String connector) {
    try {
      LOG.debug("Issuing request to Kafka Connect at URI {} to delete {}",
          connectUri, connector);

      final ConnectResponse<String> connectResponse = withRetries(() -> Request
          .Delete(connectUri.resolve(String.format("%s/%s", CONNECTORS, connector)))
          .setHeaders(headers())
          .socketTimeout(DEFAULT_TIMEOUT_MS)
          .connectTimeout(DEFAULT_TIMEOUT_MS)
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_NO_CONTENT, Object.class, foo -> connector)));

      connectResponse.error()
          .ifPresent(error -> LOG.warn("Could not delete connector: {}.", error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  private Header[] headers() {
    return authHeader.isPresent()
        ? new Header[]{new BasicHeader(HttpHeaders.AUTHORIZATION, authHeader.get())}
        : new Header[]{};
  }

  @SuppressWarnings("unchecked")
  private static <T> ConnectResponse<T> withRetries(final Callable<ConnectResponse<T>> action) {
    try {
      return RetryerBuilder.<ConnectResponse<T>>newBuilder()
          .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_ATTEMPTS))
          .withWaitStrategy(WaitStrategies.exponentialWait())
          .retryIfResult(
              result -> result == null
                  || result.httpCode() >= HttpStatus.SC_INTERNAL_SERVER_ERROR
                  || result.httpCode() == HttpStatus.SC_CONFLICT)
          .retryIfException()
          .build()
          .call(action);
    } catch (ExecutionException e) {
      // this should never happen because we retryIfException()
      throw new KsqlServerException("Unexpected exception!", e);
    } catch (RetryException e) {
      LOG.warn("Failed to query connect cluster after {} attempts.", e.getNumberOfFailedAttempts());
      if (e.getLastFailedAttempt().hasResult()) {
        return (ConnectResponse<T>) e.getLastFailedAttempt().getResult();
      }

      // should rarely happen - only if some IOException happens and we didn't
      // even get to send the request to the server
      throw new KsqlServerException(e.getCause());
    }
  }

  private static <T, C> ResponseHandler<ConnectResponse<T>> createHandler(
      final int expectedStatus,
      final Class<C> entityClass,
      final Function<C, T> cast
  ) {
    return httpResponse -> {
      final int code = httpResponse.getStatusLine().getStatusCode();
      if (httpResponse.getStatusLine().getStatusCode() != expectedStatus) {
        final String entity = EntityUtils.toString(httpResponse.getEntity());
        return ConnectResponse.failure(entity, code);
      }

      final HttpEntity entity = httpResponse.getEntity();
      final T data = cast.apply(
          entity == null
              ? null
              : MAPPER.readValue(entity.getContent(), entityClass)
      );

      return ConnectResponse.success(data, code);
    };
  }
}
