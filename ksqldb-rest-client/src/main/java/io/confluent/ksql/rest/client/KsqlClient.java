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

import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.SslConfigs;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlClient implements AutoCloseable {

  static {
    ApiJsonMapper.INSTANCE.get().registerModule(new KsqlTypesDeserializationModule());
  }

  private final Vertx vertx;
  private final HttpClient httpNonTlsClient;
  private final HttpClient httpTlsClient;
  private final LocalProperties localProperties;
  private final Optional<String> basicAuthHeader;

  public KsqlClient(
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> credentials,
      final LocalProperties localProperties,
      final HttpClientOptions httpClientOptions
  ) {
    this.vertx = Vertx.vertx();
    this.basicAuthHeader = createBasicAuthHeader(
        Objects.requireNonNull(credentials, "credentials"));
    this.localProperties = Objects.requireNonNull(localProperties, "localProperties");
    this.httpNonTlsClient = createHttpClient(vertx, clientProps, httpClientOptions, false);
    this.httpTlsClient = createHttpClient(vertx, clientProps, httpClientOptions, true);
  }

  public KsqlTarget target(final URI server) {
    final boolean isUriTls = server.getScheme().equalsIgnoreCase("https");
    final HttpClient client = isUriTls ? httpTlsClient : httpNonTlsClient;
    return new KsqlTarget(client,
        SocketAddress.inetSocketAddress(server.getPort(), server.getHost()), localProperties,
        basicAuthHeader);
  }

  public void close() {
    try {
      httpTlsClient.close();
    } catch (Exception ignore) {
      // Ignore
    }
    try {
      httpNonTlsClient.close();
    } catch (Exception ignore) {
      // Ignore
    }
    if (vertx != null) {
      vertx.close();
    }
  }

  private static Optional<String> createBasicAuthHeader(
      final Optional<BasicCredentials> credentials) {
    return credentials.map(basicCredentials -> "Basic " + Base64.getEncoder()
        .encodeToString((basicCredentials.username()
            + ":" + basicCredentials.password()).getBytes(StandardCharsets.UTF_8))
    );
  }

  private static HttpClient createHttpClient(final Vertx vertx,
      final Map<String, String> clientProps,
      final HttpClientOptions httpClientOptions,
      final boolean tls) {
    if (tls) {
      httpClientOptions.setVerifyHost(false);
      httpClientOptions.setSsl(true);
      final String trustStoreLocation = clientProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
      if (trustStoreLocation != null) {
        final String suppliedTruststorePassword = clientProps
            .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        httpClientOptions.setTrustStoreOptions(new JksOptions().setPath(trustStoreLocation)
            .setPassword(suppliedTruststorePassword == null ? "" : suppliedTruststorePassword));
        final String keyStoreLocation = clientProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (keyStoreLocation != null) {
          final String suppliedKeyStorePassord = clientProps
              .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
          httpClientOptions.setKeyStoreOptions(new JksOptions().setPath(keyStoreLocation)
              .setPassword(suppliedTruststorePassword == null ? "" : suppliedKeyStorePassord));
        }
      }
    }
    try {
      return vertx.createHttpClient(httpClientOptions);
    } catch (VertxException e) {
      throw new KsqlRestClientException(e.getMessage(), e);
    }
  }

}
