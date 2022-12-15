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
import java.util.function.Supplier;

public final class InternalKsqlClientFactory {

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
      final Supplier<HttpClientOptions> optionsSupplier) {
    return (tls) -> {
      final HttpClientOptions httpClientOptions = optionsSupplier.get();
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

  private static HttpClientOptions createClientOptions() {
    return new HttpClientOptions().setMaxPoolSize(100);
  }

  private static HttpClientOptions createClientOptionsHttp2() {
    return new HttpClientOptions().setHttp2MaxPoolSize(100).setProtocolVersion(HttpVersion.HTTP_2);
  }
}
