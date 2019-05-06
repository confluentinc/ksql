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

package io.confluent.ksql.rest.ssl;

import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.RestConfig;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.HostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.kafka.common.config.ConfigException;

public final class SslUtil {

  private SslUtil() {
  }

  static Optional<KeyStore> loadKeyStore(final Map<String, String> props) {
    return load(props,
        RestConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        RestConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        RestConfig.SSL_KEYSTORE_TYPE_CONFIG);
  }

  static Optional<KeyStore> loadTrustStore(final Map<String, String> props) {
    return load(props,
        RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        RestConfig.SSL_TRUSTSTORE_TYPE_CONFIG);
  }

  static String getKeyPassword(final Map<String, String> props) {
    return props.getOrDefault(RestConfig.SSL_KEY_PASSWORD_CONFIG, "");
  }

  static Optional<HostnameVerifier> getHostNameVerifier(final Map<String, String> props) {
    final String algo = props.getOrDefault(
        RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

    if (algo.isEmpty()) {
      return Optional.of(NoopHostnameVerifier.INSTANCE);
    }

    if (algo.equalsIgnoreCase("https")) {
      return Optional.empty();
    }

    throw new ConfigException(
        RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, algo, "Not supported");
  }

  private static Optional<KeyStore> load(
      final Map<String, String> props,
      final String locationConfig,
      final String passwordConfig,
      final String typeConfig
  ) {
    final String location = props.getOrDefault(locationConfig, "");
    if (location.isEmpty()) {
      return Optional.empty();
    }

    try (FileInputStream stream = new FileInputStream(location)) {

      final String password = props.getOrDefault(passwordConfig, "");
      final String type = props.getOrDefault(typeConfig, "JKS");

      final KeyStore keyStore = KeyStore.getInstance(type);
      keyStore.load(stream, password.toCharArray());
      return Optional.of(keyStore);
    } catch (final Exception e) {
      throw new KsqlException("Failed to load keyStore: " + location, e);
    }
  }
}
