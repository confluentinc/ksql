/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyStoreOptions;
import io.vertx.core.net.PfxOptions;
import java.security.Security;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;

public final class VertxSslOptionsFactory {
  private static final String SSL_STORE_TYPE_JKS = "JKS";
  private static final String SSL_STORE_TYPE_BCFKS = "BCFKS";


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

  private static String getKeyPassword(final Map<String, String> props) {
    return props.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
  }

  private static String getSecurityProviders(final Map<String, String> props) {
    return props.get(SecurityConfig.SECURITY_PROVIDERS_CONFIG);
  }

  private static String getKeyManagerAlgorithm(final Map<String, String> props) {
    return props.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
  }

  private static JksOptions buildJksOptions(final String path, final String password) {
    return new JksOptions().setPath(path).setPassword(Strings.nullToEmpty(password));
  }

  private static JksOptions buildJksOptions(final Buffer buffer, final String password) {
    return new JksOptions().setValue(buffer).setPassword(Strings.nullToEmpty(password));
  }

  private static KeyStoreOptions buildBcfksOptions(
      final String provider, final String path, final String password, final String aliasPassword,
      final String keyManagerAlgorithm) {

    if (!Strings.isNullOrEmpty(keyManagerAlgorithm)) {
      Security.setProperty("ssl.KeyManagerFactory.algorithm", keyManagerAlgorithm);
    }
    return new KeyStoreOptions()
        .setType(SSL_STORE_TYPE_BCFKS)
        .setProvider(provider)
        .setPath(path)
        .setPassword(Strings.nullToEmpty(password))
        .setAliasPassword(Strings.nullToEmpty(aliasPassword));
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

  public static JksOptions getJksTrustStoreOptions(final String path, final String password) {
    return buildJksOptions(path, password);
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
   * Returns a {@code KeyStoreOptions} object using the following truststore SSL configurations:
   * <ul>
   *  <li>Optional: {@value SslConfigs#SSL_TRUSTSTORE_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * @param props A Map with the truststore location and password configs.
   * @return The {@code KeyStoreOptions} configured with the above SSL settings.
   *         Optional.empty() if the truststore password is null or empty.
   */

  public static Optional<KeyStoreOptions> getBcfksKeyStoreOptions(final Map<String, String> props) {
    final String providers = getSecurityProviders(props);
    final String location = getKeyStoreLocation(props);
    final String password = getKeyStorePassword(props);
    final String keyPassword = getKeyPassword(props);
    final String keyManagerAlgorithm = getKeyManagerAlgorithm(props);

    if (!Strings.isNullOrEmpty(location)
        && !Strings.isNullOrEmpty(password)
        && !Strings.isNullOrEmpty(providers)) {
      return Optional.of(buildBcfksOptions(
          providers, location, password, keyPassword, keyManagerAlgorithm));
    }

    return Optional.empty();
  }

  public static Optional<KeyStoreOptions> getBcfksKeyStoreOptions(
      final String location, final String password, final String keyPassword) {
    return getBcfksKeyStoreOptions(ImmutableMap.of(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, location,
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword));
  }


  /**
   * Returns a {@code KeyStoreOptions} object using the following truststore SSL configurations:
   * <ul>
   *  <li>Optional: {@value SslConfigs#SSL_TRUSTSTORE_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * @param props A Map with the truststore location and password configs.
   * @return The {@code KeyStoreOptions} configured with the above SSL settings.
   *         Optional.empty() if the truststore password is null or empty.
   */

  public static Optional<KeyStoreOptions> getBcfksTrustStoreOptions(
      final Map<String, String> props) {
    final String providers = getSecurityProviders(props);
    final String location = getTrustStoreLocation(props);
    final String password = getTrustStorePassword(props);
    final String keyPassword = getKeyPassword(props);
    final String keyManagerAlgorithm = getKeyManagerAlgorithm(props);

    if (!Strings.isNullOrEmpty(location)
        && !Strings.isNullOrEmpty(password)
        && !Strings.isNullOrEmpty(providers)) {
      return Optional.of(buildBcfksOptions(
          providers, location, password, keyPassword, keyManagerAlgorithm));
    }

    return Optional.empty();
  }

  public static Optional<KeyStoreOptions> getBcfksTrustStoreOptions(
      final String location,
      final String password,
      final String keyPassword) {
    return getBcfksTrustStoreOptions(ImmutableMap.of(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, location,
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, password,
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword
    ));
  }

  /**
   * Returns a {@code JksOptions} object using the keystore SSL configurations:
   * <ul>
   *  <li>Required: {@value SslConfigs#SSL_KEYSTORE_LOCATION_CONFIG}</li>
   *  <li>Optional: {@value SslConfigs#SSL_KEYSTORE_PASSWORD_CONFIG}</li>
   *  <li>Optional: {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}</li>
   * </ul>
   *
   * <p>If an {@code alias} is used, then it builds the {@code JksOptions} with the internal
   * private key referenced with the alias. The internal private key will be decrypted using
   * the {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}.
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
    final String keyPassword = getKeyPassword(props);

    if (!Strings.isNullOrEmpty(location)) {
      final JksOptions jksOptions;

      if (alias.isPresent() && !alias.get().isEmpty()) {
        jksOptions = buildJksOptions(
            loadJksKeyStore(location, keyStorePassword, keyPassword, alias.get()),
            keyStorePassword
        );
      } else {
        jksOptions = buildJksOptions(location, keyStorePassword);
      }

      return Optional.of(jksOptions);
    }

    return Optional.empty();
  }

  public static JksOptions buildJksKeyStoreOptions(
      final String path,
      final String password,
      final Optional<String> keyPassword,
      final Optional<String> alias
  ) {
    return buildJksKeyStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, path,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword.orElse("")
        ), alias
    ).get();
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
