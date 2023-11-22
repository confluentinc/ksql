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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.test.util.KsqlTestFolder;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyStoreOptions;
import io.vertx.core.net.PfxOptions;
import java.security.Security;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@RunWith(MockitoJUnitRunner.class)
public class VertxSslOptionsFactoryTest {
  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  @ClassRule
  public static final TemporaryFolder TMP = KsqlTestFolder.temporaryFolder();

  @Test
  public void shouldBuildTrustStoreJksOptionsWithPathAndPassword() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.getJksTrustStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            "path",
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            "password"
        )
    );

    // Then
    assertThat(jksOptions.get().getPath(), is("path"));
    assertThat(jksOptions.get().getPassword(), is("password"));
  }

  @Test
  public void shouldReturnEmptyTrustStoreJksOptionsIfLocationIsEmpty() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.getJksTrustStoreOptions(
        ImmutableMap.of()
    );

    // Then
    assertThat(jksOptions, is(Optional.empty()));
  }

  @Test
  public void shouldBuildTrustStoreJksOptionsWithPathOnly() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.getJksTrustStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            "path"
        )
    );

    // Then
    assertThat(jksOptions.get().getPath(), is("path"));
    assertThat(jksOptions.get().getPassword(), is(""));
  }

  @Test
  public void shouldBuildTrustStorePfxOptionsWithPathAndPassword() {
    // When
    final Optional<PfxOptions> pfxOptions = VertxSslOptionsFactory.getPfxTrustStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            "path",
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            "password"
        )
    );

    // Then
    assertThat(pfxOptions.get().getPath(), is("path"));
    assertThat(pfxOptions.get().getPassword(), is("password"));
  }

  @Test
  public void shouldReturnEmptyTrustStorePfxOptionsIfLocationIsEmpty() {
    // When
    final Optional<PfxOptions> pfxOptions = VertxSslOptionsFactory.getPfxTrustStoreOptions(
        ImmutableMap.of()
    );

    // Then
    assertThat(pfxOptions, is(Optional.empty()));
  }

  @Test
  public void shouldBuildTrustStorePfxOptionsWithPathOnly() {
    // When
    final Optional<PfxOptions> pfxOptions = VertxSslOptionsFactory.getPfxTrustStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            "path"
        )
    );

    // Then
    assertThat(pfxOptions.get().getPath(), is("path"));
    assertThat(pfxOptions.get().getPassword(), is(""));
  }

  @Test
  public void shouldBuildTrustStoreBCFKSOptionsWithEssentialFields() {
    // When
    final Optional<KeyStoreOptions> trustStoreOptions = VertxSslOptionsFactory.getBcfksTrustStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "location",
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "password",
            SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "algorithm",
            SecurityConfig.SECURITY_PROVIDERS_CONFIG, "security-providers-list"
        )
    );

    // Then
    assertThat(trustStoreOptions.isPresent(), equalTo(true));
    assertThat(trustStoreOptions.get().getPath(), is("location"));
    assertThat(trustStoreOptions.get().getPassword(), is("password"));
    assertThat(Security.getProperty("ssl.TrustManagerFactory.algorithm"), is("algorithm"));
    assertThat(Security.getProperty("security.providers"), is("security-providers-list"));
  }

  @Test
  public void shouldReturnEmptyTrustStoreBCFKSOptionsIfPasswordIsEmpty() {
    // When
    final Optional<KeyStoreOptions> keyStoreOptions = VertxSslOptionsFactory.getBcfksTrustStoreOptions(
        ImmutableMap.of()
    );

    // Then
    assertThat(keyStoreOptions, is(Optional.empty()));
  }



  @Test
  public void shouldBuildKeyStoreJksOptionsWithPathAndPassword() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.buildJksKeyStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            "path",
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            "password"
        ),
        Optional.empty()
    );

    // Then
    assertThat(jksOptions.get().getPath(), is("path"));
    assertThat(jksOptions.get().getPassword(), is("password"));
  }

  @Test
  public void shouldReturnEmptyKeyStoreJksOptionsIfLocationIsEmpty() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.buildJksKeyStoreOptions(
        ImmutableMap.of(),
        Optional.empty()
    );

    // Then
    assertThat(jksOptions, is(Optional.empty()));
  }

  @Test
  public void shouldBuildKeyStoreJksOptionsWithPathOnly() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.buildJksKeyStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            "path"
        ),
        Optional.empty()
    );

    // Then
    assertThat(jksOptions.get().getPath(), is("path"));
    assertThat(jksOptions.get().getPassword(), is(""));
  }

  @Test
  public void shouldBuildKeyStoreJksOptionsWithKeyPasswordWhenAliasIsPassed() {
    // When
    final Optional<JksOptions> jksOptions = VertxSslOptionsFactory.buildJksKeyStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SERVER_KEY_STORE.keyStoreProps().get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SERVER_KEY_STORE.keyStoreProps().get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SERVER_KEY_STORE.keyStoreProps().get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
        ),
        Optional.of("localhost")
    );

    // Then
    assertThat(jksOptions.get().getValue(), not(nullValue()));
    assertThat(jksOptions.get().getPassword(),
        is(SERVER_KEY_STORE.keyStoreProps().get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)));

    // When a key password is set, the Vert.x options are built without a keyStore location
    assertThat(jksOptions.get().getPath(), is(nullValue()));
  }

  @Test
  public void shouldBuildKeyStorePfxOptionsWithPathAndPassword() {
    // When
    final Optional<PfxOptions> pfxOptions = VertxSslOptionsFactory.getPfxKeyStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            "path",
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            "password"
        )
    );

    // Then
    assertThat(pfxOptions.get().getPath(), is("path"));
    assertThat(pfxOptions.get().getPassword(), is("password"));
  }

  @Test
  public void shouldReturnEmptyKeyStorePfxOptionsIfLocationIsEmpty() {
    // When
    final Optional<PfxOptions> pfxOptions = VertxSslOptionsFactory.getPfxKeyStoreOptions(
        ImmutableMap.of()
    );

    // Then
    assertThat(pfxOptions, is(Optional.empty()));
  }

  @Test
  public void shouldBuildKeyStorePfxOptionsWithPathOnly() {
    // When
    final Optional<PfxOptions> pfxOptions = VertxSslOptionsFactory.getPfxKeyStoreOptions(
        ImmutableMap.of(
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            "path"
        )
    );

    // Then
    assertThat(pfxOptions.get().getPath(), is("path"));
    assertThat(pfxOptions.get().getPassword(), is(""));
  }
}
