/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.http.HttpServerOptions;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ApiServerUtilsTest {

  @Test
  public void shouldMaskKsqlRequestQuery() throws ExecutionException, InterruptedException {
    // Given
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";
    final KsqlRequest req = new KsqlRequest(query, ImmutableMap.of(), ImmutableMap.of(), 1L);

    final String expected = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    // When, Then
    assertThrows(NullPointerException.class, req::getMaskedKsql);
    ApiServerUtils.setMaskedSqlIfNeeded(req);
    assertThat(req.getMaskedKsql(), is(expected));
  }

  @Test
  public void shouldSetMaskKsql() {
     // Given
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";
    final KsqlRequest req = new KsqlRequest(query, ImmutableMap.of(), ImmutableMap.of(), 1L);

    final String expected = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    // When, Then
    ApiServerUtils.setMaskedSql(req);
    assertThat(req.getMaskedKsql(), is(expected));
  }

  @Test
  public void shouldEnableAlpnWhenFipsModeDisabled() {
    // Given: FIPS mode is NOT enabled (default config)
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.of());
    final HttpServerOptions options = new HttpServerOptions();

    // When
    ApiServerUtils.setTlsOptions(restConfig, options, null, ClientAuth.NONE);

    // Then: ALPN should be enabled (standard behavior for non-FIPS mode)
    assertThat("ALPN should be enabled when FIPS mode is disabled",
        options.isUseAlpn(), is(true));
    assertThat("SSL should be enabled", options.isSsl(), is(true));
  }

  @Test
  public void shouldConfigureAlpnBasedOnOpenSslAvailabilityInFipsMode() {
    // Given: FIPS mode is enabled
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.of(
        ConfluentConfigs.ENABLE_FIPS_CONFIG, true
    ));
    final HttpServerOptions options = new HttpServerOptions();

    // When
    ApiServerUtils.setTlsOptions(restConfig, options, null, ClientAuth.NONE);

    // Then: ALPN setting depends on OpenSSL availability
    // - If OpenSSL is available: ALPN enabled
    // - If OpenSSL is NOT available: ALPN disabled (to avoid BouncyCastle reflection errors)
    if (OpenSsl.isAvailable()) {
      assertThat("ALPN should be enabled when FIPS mode is on AND OpenSSL is available",
          options.isUseAlpn(), is(true));
    } else {
      assertThat("ALPN should be disabled when FIPS mode is on AND OpenSSL is unavailable",
          options.isUseAlpn(), is(false));
    }
    assertThat("SSL should be enabled", options.isSsl(), is(true));
  }

  @Test
  public void shouldAlwaysEnableSsl() {
    // Given
    final KsqlRestConfig restConfig = new KsqlRestConfig(ImmutableMap.of());
    final HttpServerOptions options = new HttpServerOptions();

    // When
    ApiServerUtils.setTlsOptions(restConfig, options, null, ClientAuth.NONE);

    // Then
    assertThat("SSL should always be enabled", options.isSsl(), is(true));
  }

}
