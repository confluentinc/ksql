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

package io.confluent.ksql.rest.server.services;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.client.KsqlTarget;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.util.KeystoreUtil;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlHostInfo;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DefaultKsqlClient implements SimpleKsqlClient {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultKsqlClient.class);

  private final Optional<String> authHeader;
  private final KsqlClient sharedClient;

  DefaultKsqlClient(final Optional<String> authHeader, final Map<String, Object> clientProps) {
    this(
        authHeader,
        getInternalClient(toClientProps(clientProps), SocketAddress::inetSocketAddress)
    );
  }

  @VisibleForTesting
  DefaultKsqlClient(final Optional<String> authHeader, final Map<String, Object> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory) {
    this(
        authHeader,
        getInternalClient(toClientProps(clientProps), socketAddressFactory)
    );
  }

  @VisibleForTesting
  DefaultKsqlClient(
      final Optional<String> authHeader,
      final KsqlClient sharedClient
  ) {
    this.authHeader = requireNonNull(authHeader, "authHeader");
    this.sharedClient = requireNonNull(sharedClient, "sharedClient");
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> requestProperties) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    return getTarget(target, authHeader)
        .postKsqlRequest(sql, requestProperties, Optional.empty());
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint)
        .properties(configOverrides);

    final RestResponse<List<StreamedRow>> resp = getTarget(target, authHeader)
        .postQueryRequest(sql, requestProperties, Optional.empty());

    if (resp.isErroneous()) {
      return RestResponse.erroneous(resp.getStatusCode(), resp.getErrorMessage());
    }

    return RestResponse.successful(resp.getStatusCode(), resp.getResponse());
  }

  @Override
  public void makeAsyncHeartbeatRequest(
      final URI serverEndPoint,
      final KsqlHostInfo host,
      final long timestamp) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    getTarget(target, authHeader)
        .postAsyncHeartbeatRequest(new KsqlHostInfoEntity(host.host(), host.port()), timestamp)
        .exceptionally(t -> {
          // We send heartbeat requests quite frequently and to nodes that might be down.  We don't
          // want to fill the logs with spam, so we debug log exceptions.
          LOG.debug("Exception in async heartbeat request", t);
          return null;
        });
  }

  @Override
  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest(final URI serverEndPoint) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    return getTarget(target, authHeader).getClusterStatus();
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    getTarget(target, authHeader).postAsyncLagReportingRequest(lagReportingMessage)
        .exceptionally(t -> {
          LOG.debug("Exception in async lag reporting request", t);
          return null;
        });
  }

  @Override
  public void close() {
    sharedClient.close();
  }

  private KsqlTarget getTarget(final KsqlTarget target, final Optional<String> authHeader) {
    return authHeader
        .map(target::authorizationHeader)
        .orElse(target);
  }

  private static HttpClientOptions createClientOptions() {
    return new HttpClientOptions().setMaxPoolSize(100);
  }

  private static Map<String, String> toClientProps(final Map<String, Object> config) {
    final Map<String, String> clientProps = new HashMap<>();
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      clientProps.put(entry.getKey(), entry.getValue().toString());
    }
    return clientProps;
  }

  private static Function<Boolean, HttpClientOptions> httpOptionsFactory(
      final Map<String, String> clientProps, final boolean verifyHost) {
    return (tls) -> {
      final HttpClientOptions httpClientOptions = createClientOptions();
      if (!tls) {
        return httpClientOptions;
      }
      httpClientOptions.setVerifyHost(verifyHost);
      httpClientOptions.setSsl(true);
      final String trustStoreLocation = clientProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
      if (!Strings.isNullOrEmpty(trustStoreLocation)) {
        final String suppliedTruststorePassword = clientProps
            .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        httpClientOptions.setTrustStoreOptions(new JksOptions().setPath(trustStoreLocation)
            .setPassword(Strings.nullToEmpty(suppliedTruststorePassword)));

        final String keyStoreLocation = clientProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (!Strings.isNullOrEmpty(keyStoreLocation)) {
          final String suppliedKeyStorePassword = clientProps
              .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
          final String internalAlias = clientProps
              .get(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG);
          final JksOptions keyStoreOptions = new JksOptions()
              .setPassword(Strings.nullToEmpty(suppliedKeyStorePassword));
          if (!Strings.isNullOrEmpty(internalAlias)) {
            keyStoreOptions.setValue(KeystoreUtil.getKeyStore(
                KsqlRestConfig.SSL_STORE_TYPE_JKS,
                keyStoreLocation,
                Optional.ofNullable(Strings.emptyToNull(suppliedKeyStorePassword)),
                Optional.ofNullable(Strings.emptyToNull(suppliedKeyStorePassword)),
                internalAlias));
          } else {
            keyStoreOptions.setPath(keyStoreLocation);
          }
          httpClientOptions.setKeyStoreOptions(keyStoreOptions);
        }
      }
      return httpClientOptions;
    };
  }

  private static KsqlClient getInternalClient(final Map<String, String> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory) {
    final String internalClientAuth = clientProps.get(
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG);
    final boolean verifyHost = !Strings.isNullOrEmpty(internalClientAuth)
        && !KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE.equals(internalClientAuth);

    return new KsqlClient(
        Optional.empty(),
        new LocalProperties(ImmutableMap.of()),
        httpOptionsFactory(clientProps, verifyHost),
        socketAddressFactory
    );
  }
}
