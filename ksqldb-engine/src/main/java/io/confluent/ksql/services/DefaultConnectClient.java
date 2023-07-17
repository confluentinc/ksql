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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.QueryMask;
import io.vertx.core.http.HttpHeaders;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.util.Timeout;
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

  private static final ObjectMapper MAPPER = ConnectJsonMapper.INSTANCE.get();

  private static final String CONNECTORS = "/connectors";
  private static final String STATUS = "/status";
  private static final String TOPICS = "/topics";
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
    } catch (final URISyntaxException e) {
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
      final Map<String, String> maskedConfig = QueryMask.getMaskedConnectConfig(config);
      LOG.debug("Issuing create request to Kafka Connect at URI {} with name {} and config {}",
          connectUri,
          connector,
          maskedConfig);

      final ConnectResponse<ConnectorInfo> connectResponse = withRetries(() -> Request
          .post(connectUri.resolve(CONNECTORS))
          .setHeaders(headers())
          .responseTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .connectTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .bodyString(
              MAPPER.writeValueAsString(
                  ImmutableMap.of(
                      "name", connector,
                      "config", config)),
              ContentType.APPLICATION_JSON
          )
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_CREATED, new TypeReference<ConnectorInfo>() {},
                  Function.identity())));

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
          .get(connectUri.resolve(CONNECTORS))
          .setHeaders(headers())
          .responseTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .connectTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_OK, new TypeReference<List<String>>() {},
                  Function.identity())));


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
          .get(connectUri.resolve(CONNECTORS + "/" + connector + STATUS))
          .setHeaders(headers())
          .responseTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .connectTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_OK, new TypeReference<ConnectorStateInfo>() {},
                  Function.identity())));

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
          .get(connectUri.resolve(String.format("%s/%s", CONNECTORS, connector)))
          .setHeaders(headers())
          .responseTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .connectTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_OK, new TypeReference<ConnectorInfo>() {},
                  Function.identity())));


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
          .delete(connectUri.resolve(String.format("%s/%s", CONNECTORS, connector)))
          .setHeaders(headers())
          .responseTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .connectTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
          .execute()
          .handleResponse(
              createHandler(HttpStatus.SC_NO_CONTENT, new TypeReference<Object>() {},
                  foo -> connector)));


      connectResponse.error()
          .ifPresent(error -> LOG.warn("Could not delete connector: {}.", error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  @Override
  public ConnectResponse<Map<String, Map<String, List<String>>>> topics(final String connector) {
    try {
      LOG.debug("Issuing request to Kafka Connect at URI {} to get active topics for {}",
          connectUri, connector);

      final ConnectResponse<Map<String, Map<String, List<String>>>> connectResponse = withRetries(
          () -> Request.get(connectUri.resolve(CONNECTORS + "/" + connector + TOPICS))
              .setHeaders(headers())
              .responseTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
              .connectTimeout(Timeout.ofMilliseconds(DEFAULT_TIMEOUT_MS))
              .execute()
              .handleResponse(
                  createHandler(HttpStatus.SC_OK,
                      new TypeReference<Map<String, Map<String, List<String>>>>() {},
                      Function.identity())));

      connectResponse.error()
          .ifPresent(
              error -> LOG.warn("Could not query topics of connector {}: {}", connector, error));

      return connectResponse;
    } catch (final Exception e) {
      throw new KsqlServerException(e);
    }
  }

  private Header[] headers() {
    return authHeader.isPresent()
        ? new Header[]{new BasicHeader(HttpHeaders.AUTHORIZATION.toString(), authHeader.get())}
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
    } catch (final ExecutionException e) {
      // this should never happen because we retryIfException()
      throw new KsqlServerException("Unexpected exception!", e);
    } catch (final RetryException e) {
      LOG.warn("Failed to query connect cluster after {} attempts.", e.getNumberOfFailedAttempts());
      if (e.getLastFailedAttempt().hasResult()) {
        return (ConnectResponse<T>) e.getLastFailedAttempt().getResult();
      }

      // should rarely happen - only if some IOException happens and we didn't
      // even get to send the request to the server
      throw new KsqlServerException(e.getCause());
    }
  }

  private static <T, C> HttpClientResponseHandler<ConnectResponse<T>> createHandler(
      final int expectedStatus,
      final TypeReference<C> entityTypeRef,
      final Function<C, T> cast
  ) {
    return httpResponse -> {
      final int code = httpResponse.getCode();
      if (httpResponse.getCode() != expectedStatus) {
        final String entity = EntityUtils.toString(httpResponse.getEntity());
        return ConnectResponse.failure(entity, code);
      }

      final HttpEntity entity = httpResponse.getEntity();
      final T data = cast.apply(
          entity == null
              ? null
              : MAPPER.readValue(entity.getContent(), entityTypeRef)
      );

      return ConnectResponse.success(data, code);
    };
  }
}
