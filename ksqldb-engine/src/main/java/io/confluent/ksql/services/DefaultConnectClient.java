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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.ConfigInfos;
import io.confluent.ksql.rest.entity.ConnectorInfo;
import io.confluent.ksql.rest.entity.ConnectorStateInfo;
import io.confluent.ksql.rest.entity.SimpleConnectorPluginInfo;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.QueryMask;
import io.vertx.core.http.HttpHeaders;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default implementation of {@code ConnectClient}. This implementation is
 * thread safe, and the methods are all <i>blocking</i> and are configured with
 * timeouts configurable via
 * {@link io.confluent.ksql.util.KsqlConfig#CONNECT_REQUEST_TIMEOUT_MS}.
 */
public class DefaultConnectClient implements ConnectClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultConnectClient.class);

  private static final ObjectMapper MAPPER = ConnectJsonMapper.INSTANCE.get();

  private static final String CONNECTOR_PLUGINS = "/connector-plugins";
  private static final String CONNECTORS = "/connectors";
  private static final String STATUS = "/status";
  private static final String TOPICS = "/topics";
  private static final String VALIDATE_CONNECTOR = CONNECTOR_PLUGINS + "/%s/config/validate";
  private static final int MAX_ATTEMPTS = 3;

  private final URI connectUri;
  private final Header[] requestHeaders;
  private final CloseableHttpClient httpClient;
  private final long requestTimeoutMs;

  public DefaultConnectClient(
      final String connectUri,
      final Optional<String> authHeader,
      final Map<String, String> additionalRequestHeaders,
      final Optional<SSLContext> sslContext,
      final boolean verifySslHostname,
      final long requestTimeoutMs
  ) {
    Objects.requireNonNull(connectUri, "connectUri");
    Objects.requireNonNull(authHeader, "authHeader");
    Objects.requireNonNull(additionalRequestHeaders, "additionalRequestHeaders");
    Objects.requireNonNull(sslContext, "sslContext");
    try {
      this.connectUri = new URI(connectUri);
    } catch (final URISyntaxException e) {
      throw new KsqlException(
          "Could not initialize connect client due to invalid URI: " + connectUri, e);
    }
    this.requestHeaders = buildHeaders(authHeader, additionalRequestHeaders);
    this.httpClient = buildHttpClient(sslContext, verifySslHostname);
    this.requestTimeoutMs = requestTimeoutMs;
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
          .post(resolveUri(CONNECTORS))
          .setHeaders(requestHeaders)
          .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .bodyString(
              MAPPER.writeValueAsString(
                  ImmutableMap.of(
                      "name", connector,
                      "config", config)),
              ContentType.APPLICATION_JSON
          )
          .execute(httpClient)
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

  @Override
  public ConnectResponse<ConfigInfos> validate(
      final String plugin,
      final Map<String, String> config) {
    try {
      final Map<String, String> maskedConfig = QueryMask.getMaskedConnectConfig(config);
      LOG.debug("Issuing validate request to Kafka Connect at URI {} for plugin {} and config {}",
          connectUri,
          plugin,
          maskedConfig);

      final ConnectResponse<ConfigInfos> connectResponse = withRetries(() -> Request
          .put(resolveUri(String.format(VALIDATE_CONNECTOR, plugin)))
          .setHeaders(requestHeaders)
          .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .bodyString(MAPPER.writeValueAsString(config), ContentType.APPLICATION_JSON)
          .execute(httpClient)
          .handleResponse(
              createHandler(HttpStatus.SC_OK, new TypeReference<ConfigInfos>() {},
                  Function.identity())));

      connectResponse.error()
          .ifPresent(error ->
              LOG.warn("Did not VALIDATE connector configuration for plugin {} and config {}: {}",
              plugin, maskedConfig, error));

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
          .get(resolveUri(CONNECTORS))
          .setHeaders(requestHeaders)
          .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .execute(httpClient)
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
  public ConnectResponse<List<SimpleConnectorPluginInfo>> connectorPlugins() {
    try {
      LOG.debug("Issuing request to Kafka Connect at URI {} to list connector plugins", connectUri);

      final ConnectResponse<List<SimpleConnectorPluginInfo>> connectResponse =
          withRetries(() -> Request
              .get(resolveUri(CONNECTOR_PLUGINS))
              .setHeaders(requestHeaders)
              .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
              .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
              .execute(httpClient)
              .handleResponse(
                  createHandler(
                      HttpStatus.SC_OK,
                      new TypeReference<List<SimpleConnectorPluginInfo>>() {},
                      Function.identity())));

      connectResponse.error()
          .ifPresent(error -> LOG.warn("Could not list connector plugins: {}.", error));

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
          .get(resolveUri(CONNECTORS + "/" + connector + STATUS))
          .setHeaders(requestHeaders)
          .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .execute(httpClient)
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
          .get(resolveUri(String.format("%s/%s", CONNECTORS, connector)))
          .setHeaders(requestHeaders)
          .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .execute(httpClient)
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
          .delete(resolveUri(String.format("%s/%s", CONNECTORS, connector)))
          .setHeaders(requestHeaders)
          .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
          .execute(httpClient)
          .handleResponse(
              createHandler(
                  ImmutableList.of(HttpStatus.SC_NO_CONTENT, HttpStatus.SC_OK),
                  new TypeReference<Object>() {},
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
          () -> Request.get(resolveUri(CONNECTORS + "/" + connector + TOPICS))
              .setHeaders(requestHeaders)
              .responseTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
              .connectTimeout(Timeout.ofMilliseconds(requestTimeoutMs))
              .execute(httpClient)
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

  @VisibleForTesting
  public Header[] getRequestHeaders() {
    return requestHeaders.clone();
  }

  private String resolveUri(final String relativePath) {
    try {
      return new URI(
          connectUri.getScheme(),
          connectUri.getUserInfo(),
          connectUri.getHost(),
          connectUri.getPort(),
          // concatenate relative path to existing path in order to support relative resolution;
          // in contrast, URI.resolve() will resolve a path such as `/connectors` as an
          // absolute path only.
          Paths.get(connectUri.getPath(), relativePath).toString(),
          connectUri.getQuery(),
          connectUri.getFragment()).toString();
    } catch (URISyntaxException e) {
      throw new KsqlServerException("Failed to resolve URI", e);
    }
  }

  private static Header[] buildHeaders(
      final Optional<String> authHeader,
      final Map<String, String> additionalRequestHeaders
  ) {
    final List<Header> headers = new ArrayList<>();

    authHeader.ifPresent(header -> headers.add(
        new BasicHeader(HttpHeaders.AUTHORIZATION.toString(), authHeader.get())));

    if (!additionalRequestHeaders.isEmpty()) {
      final List<Header> additionalHeaders = additionalRequestHeaders.entrySet().stream()
          .map(e -> new BasicHeader(e.getKey(), e.getValue()))
          .collect(Collectors.toList());
      headers.addAll(additionalHeaders);
    }

    return headers.toArray(new Header[0]);
  }

  /**
   * Uses defaults from Request.execute(), except with an explicit SSLSocketFactory to pass
   * custom SSL configs. Link to default below:
   * https://github.com/apache/httpcomponents-client/blob/3734aaa038a58c17af638e9fa29019cacb22e82c/httpclient5-fluent/src/main/java/org/apache/hc/client5/http/fluent/Executor.java#L62-L72
   */
  private static CloseableHttpClient buildHttpClient(
      final Optional<SSLContext> sslContext,
      final boolean verifySslHostname
  ) {
    final PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create();
    sslContext.ifPresent(ctx -> {
      final SSLConnectionSocketFactory socketFactory = verifySslHostname
          ? new SSLConnectionSocketFactory(ctx)
          : new SSLConnectionSocketFactory(ctx, (hostname, session) -> true);
      connectionManagerBuilder.setSSLSocketFactory(socketFactory);
    });

    return HttpClientBuilder.create()
        .setConnectionManager(connectionManagerBuilder
            .setMaxConnPerRoute(100)
            .setMaxConnTotal(200)
            .setValidateAfterInactivity(TimeValue.ofSeconds(10L))
            .build())
        .useSystemProperties()
        .evictExpiredConnections()
        .evictIdleConnections(TimeValue.ofMinutes(1L))
        .build();
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
    return createHandler(Collections.singletonList(expectedStatus), entityTypeRef, cast);
  }

  private static <T, C> HttpClientResponseHandler<ConnectResponse<T>> createHandler(
      final List<Integer> expectedStatuses,
      final TypeReference<C> entityTypeRef,
      final Function<C, T> cast
  ) {
    return httpResponse -> {
      final int code = httpResponse.getCode();
      if (!expectedStatuses.contains(httpResponse.getCode())) {
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
