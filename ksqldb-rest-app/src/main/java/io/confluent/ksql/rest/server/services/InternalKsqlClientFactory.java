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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.KsqlClient;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.common.config.SslConfigs;

public final class InternalKsqlClientFactory {

  private InternalKsqlClientFactory() {}

  public static KsqlClient createInternalClient(
      final Map<String, String> clientProps,
      final BiFunction<Integer, String, SocketAddress> socketAddressFactory,
      final Vertx vertx) {
    return new KsqlClient(
        Optional.empty(),
        new LocalProperties(ImmutableMap.of()),
        httpOptionsFactory(clientProps),
        socketAddressFactory,
        vertx
    );
  }

  private static Function<Boolean, HttpClientOptions> httpOptionsFactory(
      final Map<String, String> clientProps) {
    return (tls) -> {
      final HttpClientOptions httpClientOptions = createClientOptions();
      if (!tls) {
        return httpClientOptions;
      }

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
      return httpClientOptions;
    };
  }

  private static HttpClientOptions createClientOptions() {
    return new HttpClientOptions().setMaxPoolSize(100);
  }
}
