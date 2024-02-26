/*
 * Copyright 2020 Confluent Inc.
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.VertxSslOptionsFactory;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InternalKsqlClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(InternalKsqlClientFactory.class);

  private InternalKsqlClientFactory() {

  }

  public static KsqlClient createInternalClient(
      final Map<String, String> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory,
      final Vertx vertx) {
    final String internalClientAuth = clientProps.get(
        KsqlRestConfig.KSQL_INTERNAL_SSL_CLIENT_AUTHENTICATION_CONFIG);
    final boolean verifyHost = !Strings.isNullOrEmpty(internalClientAuth)
        && !KsqlRestConfig.SSL_CLIENT_AUTHENTICATION_NONE.equals(internalClientAuth);

    return new KsqlClient(
        Optional.empty(),
        new LocalProperties(ImmutableMap.of()),
        httpOptionsFactory(clientProps, verifyHost, InternalKsqlClientFactory::createClientOptions),
        httpOptionsFactory(clientProps, verifyHost,
            InternalKsqlClientFactory::createClientOptionsHttp2),
        socketAddressFactory,
        vertx
    );
  }

  private static Function<Boolean, HttpClientOptions> httpOptionsFactory(
      final Map<String, String> clientProps, final boolean verifyHost,
      final BiFunction<Map<String, String>, Boolean, HttpClientOptions> clientOptions) {
    return (tls) -> {
      final HttpClientOptions httpClientOptions = clientOptions.apply(clientProps, tls);
      if (!tls) {
        return httpClientOptions;
      }
      httpClientOptions.setVerifyHost(verifyHost);
      httpClientOptions.setSsl(true);

      final Optional<JksOptions> trustStoreOptions =
          VertxSslOptionsFactory.getJksTrustStoreOptions(clientProps);

      if (trustStoreOptions.isPresent()) {
        httpClientOptions.setTrustStoreOptions(trustStoreOptions.get());

        final Optional<JksOptions> keyStoreOptions =
            VertxSslOptionsFactory.buildJksKeyStoreOptions(
                clientProps,
                Optional.ofNullable(
                    clientProps.get(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG))
            );

        keyStoreOptions.ifPresent(options -> httpClientOptions.setKeyStoreOptions(options));
      }

      return httpClientOptions;
    };
  }

  private static HttpClientOptions createClientOptions(
      final Map<String, String> clientProps,
      final boolean tls
  ) {
    return new HttpClientOptions().setMaxPoolSize(100);
  }

  private static HttpClientOptions createClientOptionsHttp2(
      final Map<String, String> clientProps,
      final boolean tls
  ) {
    final String size = clientProps.get(
        KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_CONFIG);
    int sizeInt;
    if (size != null) {
      try {
        sizeInt = Integer.parseInt(size);
      } catch (NumberFormatException e) {
        LOG.error("Bad int passed in for " + KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_CONFIG
            + ", using " + KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_DEFAULT, e);
        sizeInt = KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_DEFAULT;
      }
    } else {
      sizeInt = KsqlRestConfig.KSQL_INTERNAL_HTTP2_MAX_POOL_SIZE_DEFAULT;
    }
    return new HttpClientOptions()
        // At the moment, we cannot asynchronously end long-running queries in http2, in a way that
        // we can with http1.1, by just closing the connection. For that reason, we've disabled
        // multiplexing: https://github.com/confluentinc/ksql/issues/8505
        .setHttp2MultiplexingLimit(1)
        .setHttp2MaxPoolSize(sizeInt)
        .setProtocolVersion(HttpVersion.HTTP_2)
        .setUseAlpn(tls);
  }
}
