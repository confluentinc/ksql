/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.base.Strings;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PfxOptions;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.SslConfigs;

public final class VertxSslOptionsFactory {
  private static final String SSL_STORE_TYPE_JKS = "JKS";

  private VertxSslOptionsFactory() {
  }

  private static String getTrustStoreLocation(final Map<String, String> props) {
    return props.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
  }

  private static String getTrustStorePassword(final Map<String, String> props) {
    return props.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
  }

  private static String getKeyStoreLocation(final Map<String, String> props) {
    return props.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
  }

  private static String getKeyStorePassword(final Map<String, String> props) {
    return props.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
  }

  private static JksOptions buildJksOptions(final String path, final String password) {
    return new JksOptions().setPath(path).setPassword(Strings.nullToEmpty(password));
  }

  private static JksOptions buildJksOptions(final Buffer buffer, final String password) {
    return new JksOptions().setValue(buffer).setPassword(Strings.nullToEmpty(password));
  }

  private static Buffer loadJksKeyStore(
      final String path,
      final String keyStorePassword,
      final String keyPassword,
      final String alias
  ) {
    return KeystoreUtil.getKeyStore(
        SSL_STORE_TYPE_JKS,
        path,
        Optional.ofNullable(Strings.emptyToNull(keyStorePassword)),
        Optional.ofNullable(Strings.emptyToNull(keyPassword)),
        alias
    );
  }

  private static PfxOptions buildPfxOptions(final String path, final String password) {
    return new PfxOptions().setPath(path).setPassword(Strings.nullToEmpty(password));
  }

  /**
   * Returns a {@code JksOptions} object using the truststore following SSL configurations:
   * <ul>
   *  <li>Required: {@value SslConfigs#SSL_TRUSTSTORE_LOCATION_CONFIG}</li>
   *  <li>Optional: {@value SslConfigs#SSL_TRUSTSTORE_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * @param props A Map with the truststore location and password configs.
   * @return The {@code JksOptions} configured with the above SSL settings.
   *         Optional.empty() if the truststore location is null or empty.
   */
  public static Optional<JksOptions> getJksTrustStoreOptions(final Map<String, String> props) {
    final String location = getTrustStoreLocation(props);
    final String password = getTrustStorePassword(props);

    if (!Strings.isNullOrEmpty(location)) {
      return Optional.of(buildJksOptions(location, password));
    }

    return Optional.empty();
  }

  /**
   * Returns a {@code PfxOptions} object using the following truststore SSL configurations:
   * <ul>
   *  <li>Required: {@value SslConfigs#SSL_TRUSTSTORE_LOCATION_CONFIG}</li>
   *  <li>Optional: {@value SslConfigs#SSL_TRUSTSTORE_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * @param props A Map with the truststore location and password configs.
   * @return The {@code PfxOptions} configured with the above SSL settings.
   *         Optional.empty() if the truststore location is null or empty.
   */
  public static Optional<PfxOptions> getPfxTrustStoreOptions(final Map<String, String> props) {
    final String location = getTrustStoreLocation(props);
    final String password = getTrustStorePassword(props);

    if (!Strings.isNullOrEmpty(location)) {
      return Optional.of(buildPfxOptions(location, password));
    }

    return Optional.empty();
  }

  /**
   * Returns a {@code JksOptions} object using the keystore SSL configurations:
   * <ul>
   *  <li>Required: {@value SslConfigs#SSL_KEYSTORE_LOCATION_CONFIG}</li>
   *  <li>Optional: {@value SslConfigs#SSL_KEYSTORE_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * <p>If an {@code alias} is used, then it builds the {@code JksOptions} with the internal
   * private key referenced with the alias. The internal private key will be decrypted using
   * the same keystore password.
   *
   * @param props A Map with the keystore location and password configs.
   * @return The {@code JksOptions} configured with the above SSL settings.
   *         Optional.empty() if the truststore location is null or empty.
   */
  public static Optional<JksOptions> buildJksKeyStoreOptions(
      final Map<String, String> props,
      final Optional<String> alias
  ) {
    final String location = getKeyStoreLocation(props);
    final String keyStorePassword = getKeyStorePassword(props);

    if (!Strings.isNullOrEmpty(location)) {
      final JksOptions jksOptions;

      if (alias.isPresent() && !alias.get().isEmpty()) {
        jksOptions = buildJksOptions(
            loadJksKeyStore(location, keyStorePassword, keyStorePassword, alias.get()),
            keyStorePassword
        );
      } else {
        jksOptions = buildJksOptions(location, keyStorePassword);
      }

      return Optional.of(jksOptions);
    }

    return Optional.empty();
  }

  /**
   * Returns a {@code PfxOptions} object using the keystore SSL configurations:
   * <ul>
   *  <li>Required: {@value SslConfigs#SSL_KEYSTORE_LOCATION_CONFIG}</li>
   *  <li>Optional: {@value SslConfigs#SSL_KEYSTORE_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * @param props A Map with the keystore location and password configs.
   * @return The {@code PfxOptions} configured with the above SSL settings.
   *         Optional.empty() if the truststore location is null or empty.
   */
  public static Optional<PfxOptions> getPfxKeyStoreOptions(final Map<String, String> props) {
    // PFX key stores do not have a Private key password
    final String location = getKeyStoreLocation(props);
    final String password = getKeyStorePassword(props);

    if (!Strings.isNullOrEmpty(location)) {
      return Optional.of(buildPfxOptions(location, password));
    }

    return Optional.empty();
  }
}
