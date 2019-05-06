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

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.KsqlException;
import io.confluent.rest.RestConfig;
import java.security.KeyStore;
import java.util.Map;
import java.util.Optional;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SslUtilTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldNotLoadKeyStoreByDefault() {
    // When:
    final Optional<KeyStore> result = SslUtil.loadKeyStore(emptyMap());

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldNotLoadTrustStoreByDefault() {
    // When:
    final Optional<KeyStore> result = SslUtil.loadTrustStore(emptyMap());

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldLoadKeyStore() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        keyStoreProp(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
        RestConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        keyStoreProp(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)
    );

    // When:
    final Optional<KeyStore> result = SslUtil.loadKeyStore(props);

    // Then:
    assertThat(result, is(not(Optional.empty())));
  }

  @Test
  public void shouldLoadTrustStore() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        trustStoreProp(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
        RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        trustStoreProp(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
    );

    // When:
    final Optional<KeyStore> result = SslUtil.loadTrustStore(props);

    // Then:
    assertThat(result, is(not(Optional.empty())));
  }

  @Test
  public void shouldThrowIfKeyStoreNotFound() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_KEYSTORE_LOCATION_CONFIG, "/will/not/find/me"
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to load keyStore: /will/not/find/me");

    // When:
    SslUtil.loadKeyStore(props);
  }

  @Test
  public void shouldThrowIfTrustStoreNotFound() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, "/will/not/find/me"
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to load keyStore: /will/not/find/me");

    // When:
    SslUtil.loadTrustStore(props);
  }

  @Test
  public void shouldThrowIfKeyStorePasswordWrong() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_KEYSTORE_LOCATION_CONFIG,
        keyStoreProp(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
        RestConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
        "wrong!"
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to load keyStore:");
    expectedException.expectCause(hasMessage(is(
        "Keystore was tampered with, or password was incorrect")));

    // When:
    SslUtil.loadKeyStore(props);
  }

  @Test
  public void shouldThrowIfTrustStorePasswordWrong() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
        trustStoreProp(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
        RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
        "wrong!"
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Failed to load keyStore:");
    expectedException.expectCause(hasMessage(is(
        "Keystore was tampered with, or password was incorrect")));

    // When:
    SslUtil.loadTrustStore(props);
  }

  @Test
  public void shouldDefaultToNoKeyPassword() {
    assertThat(SslUtil.getKeyPassword(emptyMap()), is(""));
  }

  @Test
  public void shouldExtractKeyPassword() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_KEY_PASSWORD_CONFIG, "let me in"
    );

    // Then:
    assertThat(SslUtil.getKeyPassword(props), is("let me in"));
  }

  @Test
  public void shouldDefaultToNoopHostNameVerification() {
    assertThat(SslUtil.getHostNameVerifier(emptyMap()),
        is(Optional.of(NoopHostnameVerifier.INSTANCE)));
  }

  @Test
  public void shouldSupportNoOpHostNameVerifier() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""
    );

    // Then:
    assertThat(SslUtil.getHostNameVerifier(props),
        is(Optional.of(NoopHostnameVerifier.INSTANCE)));
  }

  @Test
  public void shouldSupportHttpsHostNameVerifier() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "httpS"
    );

    // Then:
    assertThat(SslUtil.getHostNameVerifier(props), is(Optional.empty()));
  }

  @Test
  public void shouldThrowOnUnsupportedHostNameVerifier() {
    // Given:
    final Map<String, String> props = ImmutableMap.of(
        RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "what?"
    );

    // Then:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage(
        "Invalid value what? for configuration ssl.endpoint.identification.algorithm: Not supported");

    // When:
    SslUtil.getHostNameVerifier(props);
  }

  private static String keyStoreProp(final String config) {
    return ServerKeyStore.keyStoreProps().get(config);
  }

  private static String trustStoreProp(final String config) {
    return ClientTrustStore.trustStoreProps().get(config);
  }
}
