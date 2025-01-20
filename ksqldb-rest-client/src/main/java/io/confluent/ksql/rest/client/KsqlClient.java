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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.config.SslConfigs;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlClient implements AutoCloseable {
  public static final String SSL_KEYSTORE_ALIAS_CONFIG = "ssl.keystore.alias";

  static {
    initialize();
  }

  private final Vertx vertx;
  private final HttpClient httpNonTlsClient;
  private final HttpClient httpTlsClient;
  private final Optional<HttpClient> httpNonTlsClientHttp2;
  private final Optional<HttpClient> httpTlsClientHttp2;
  private final LocalProperties localProperties;
  private final Optional<String> basicAuthHeader;
  private final BiFunction<Integer, String, SocketAddress> socketAddressFactory;
  private final boolean ownedVertx;

  /**
   * Creates a new KsqlClient.
   * @param clientProps Client properties from which to read TLS setup configs
   * @param credentials Optional credentials to pass along with requests if auth is enabled
   * @param localProperties The set of local properties to pass along to /ksql requests
   * @param httpClientOptions Default HttpClientOptions to be used when creating the client
   * @param httpClientOptionsHttp2 HttpClientOptions to be used for HTTP/2 connections
   */
  public KsqlClient(
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> credentials,
      final LocalProperties localProperties,
      final HttpClientOptions httpClientOptions,
      final Optional<HttpClientOptions> httpClientOptionsHttp2
  ) {
    this.vertx = Vertx.vertx();
    this.basicAuthHeader = createBasicAuthHeader(
        Objects.requireNonNull(credentials, "credentials"));
    this.localProperties = Objects.requireNonNull(localProperties, "localProperties");
    this.socketAddressFactory = SocketAddress::inetSocketAddress;
    this.httpNonTlsClient = createHttpClient(vertx, clientProps, httpClientOptions, false);
    this.httpTlsClient = createHttpClient(vertx, clientProps, httpClientOptions, true);
    this.httpNonTlsClientHttp2 = httpClientOptionsHttp2.map(
        options -> createHttpClient(vertx, clientProps, validateHttp2(options), false));
    this.httpTlsClientHttp2 = httpClientOptionsHttp2.map(
        options -> createHttpClient(vertx, clientProps, validateHttp2(options), true));
    this.ownedVertx = true;
  }

  /**
   * Creates a new KsqlClient.
   * @param credentials Optional credentials to pass along with requests if auth is enabled
   * @param localProperties The set of local properties to pass along to /ksql requests
   * @param httpClientOptionsFactory A factory for creating HttpClientOptions which take a parameter
   *                                 isTls, indicating whether the factory should prepare the
   *                                 options for a TLS connection
   * @param httpClientOptionsFactory2 same as above, but for HTTP/2 connections
   * @param socketAddressFactory A factoring for creating a SocketAddress, given the port and host
   *                             it's meant to represent
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public KsqlClient(
      final Optional<BasicCredentials> credentials,
      final LocalProperties localProperties,
      final Function<Boolean, HttpClientOptions> httpClientOptionsFactory,
      final Function<Boolean, HttpClientOptions> httpClientOptionsFactory2,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory,
      final Vertx vertx
  ) {
    this.vertx = vertx;
    this.basicAuthHeader = createBasicAuthHeader(
        Objects.requireNonNull(credentials, "credentials"));
    this.localProperties = Objects.requireNonNull(localProperties, "localProperties");
    this.socketAddressFactory = Objects.requireNonNull(
        socketAddressFactory, "socketAddressFactory");
    this.httpNonTlsClient = createHttpClient(vertx, httpClientOptionsFactory, false);
    this.httpTlsClient = createHttpClient(vertx, httpClientOptionsFactory, true);
    this.httpNonTlsClientHttp2 = Optional.of(
        createHttpClient(vertx, validateHttp2(httpClientOptionsFactory2), false));
    this.httpTlsClientHttp2 = Optional.of(
        createHttpClient(vertx, validateHttp2(httpClientOptionsFactory2), true));
    this.ownedVertx = false;
  }

  public KsqlTarget target(final URI server) {
    return target(server, Collections.emptyMap());
  }

  public KsqlTarget target(final URI server, final Map<String, String> additionalHeaders) {
    final boolean isUriTls = server.getScheme().equalsIgnoreCase("https");
    final HttpClient client = isUriTls ? httpTlsClient : httpNonTlsClient;
    return new KsqlTarget(client,
        socketAddressFactory.apply(server.getPort(), server.getHost()), localProperties,
        basicAuthHeader, server.getHost(), server.getPath(), additionalHeaders, RequestOptions.DEFAULT_TIMEOUT);
  }

  public KsqlTarget targetHttp2(final URI server) {
    final boolean isUriTls = server.getScheme().equalsIgnoreCase("https");
    final HttpClient client = (isUriTls ? httpTlsClientHttp2 : httpNonTlsClientHttp2).orElseThrow(
        () -> new IllegalStateException("Must provide http2 options to use targetHttp2"));
    return new KsqlTarget(client,
        socketAddressFactory.apply(server.getPort(), server.getHost()), localProperties,
        basicAuthHeader, server.getHost(), server.getPath(), Collections.emptyMap(), RequestOptions.DEFAULT_TIMEOUT);
  }

  @VisibleForTesting
  public static void initialize() {
    ApiJsonMapper.INSTANCE.get().registerModule(new KsqlTypesDeserializationModule());
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
    if (vertx != null && ownedVertx) {
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
      httpClientOptions.setSsl(true);

      configureHostVerification(clientProps, httpClientOptions);

      final Optional<JksOptions> trustStoreOptions =
          VertxSslOptionsFactory.getJksTrustStoreOptions(clientProps);

      if (trustStoreOptions.isPresent()) {
        httpClientOptions.setTrustStoreOptions(trustStoreOptions.get());

        final String alias = clientProps.get(SSL_KEYSTORE_ALIAS_CONFIG);
        final Optional<JksOptions> keyStoreOptions =
            VertxSslOptionsFactory.buildJksKeyStoreOptions(clientProps, Optional.ofNullable(alias));

        keyStoreOptions.ifPresent(options -> httpClientOptions.setKeyStoreOptions(options));
      }
    }
    try {
      return vertx.createHttpClient(httpClientOptions);
    } catch (VertxException e) {
      throw new KsqlRestClientException(e.getMessage(), e);
    }
  }

  private static HttpClient createHttpClient(final Vertx vertx,
      final Function<Boolean, HttpClientOptions> httpClientOptionsFactory,
      final boolean tls) {
    try {
      return vertx.createHttpClient(httpClientOptionsFactory.apply(tls));
    } catch (VertxException e) {
      throw new KsqlRestClientException(e.getMessage(), e);
    }
  }

  private static void configureHostVerification(
      final Map<String, String> clientProps,
      final HttpClientOptions httpClientOptions
  ) {
    final String endpointIdentificationAlg =
        clientProps.get(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
    if (!Strings.isNullOrEmpty(endpointIdentificationAlg)) {
      if (!endpointIdentificationAlg.toLowerCase().equals("https")) {
        throw new IllegalArgumentException("Config '"
            + SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
            + "' must be either 'https' or empty. Got: " + endpointIdentificationAlg);
      }

      httpClientOptions.setVerifyHost(true);
      return;
    }

    httpClientOptions.setVerifyHost(false);
  }

  private static HttpClientOptions validateHttp2(final HttpClientOptions httpClientOptions) {
    if (httpClientOptions.getProtocolVersion() != HttpVersion.HTTP_2) {
      throw new IllegalArgumentException("Expecting http2 protocol version");
    }
    return httpClientOptions;
  }

  private static Function<Boolean, HttpClientOptions> validateHttp2(
      final Function<Boolean, HttpClientOptions> httpClientOptionsFactory
  ) {
    return tls -> validateHttp2(httpClientOptionsFactory.apply(tls));
  }
}
