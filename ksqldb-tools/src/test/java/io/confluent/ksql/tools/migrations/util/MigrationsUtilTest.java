/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations.util;

import static io.confluent.ksql.tools.migrations.util.MigrationsUtil.createClientOptions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.client.ClientOptions;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class MigrationsUtilTest {

  private static final String NON_TLS_URL = "http://localhost:8088";
  private static final String TLS_URL = "https://localhost:8088";

  @Test
  public void shouldCreateNonTlsClientOptions() {
    // Given:
    final ClientOptions clientOptions = createClientOptions(NON_TLS_URL, "user",
        "pass", null, "", null,
        null, "", "foo", false, true, null);

    // Then:
    assertThat(clientOptions.isUseTls(), is(false));
    assertThat(clientOptions.getBasicAuthUsername(), is("user"));
    assertThat(clientOptions.getBasicAuthPassword(), is("pass"));
    assertThat(clientOptions.getTrustStore(), is(""));
    assertThat(clientOptions.getTrustStorePassword(), is(""));
    assertThat(clientOptions.getKeyStore(), is(""));
    assertThat(clientOptions.getKeyStorePassword(), is(""));
    assertThat(clientOptions.getKeyPassword(), is(""));
    assertThat(clientOptions.getKeyAlias(), is(""));
    assertThat(clientOptions.isUseAlpn(), is(false));
    assertThat(clientOptions.isVerifyHost(), is(true));
    assertThat(clientOptions.getRequestHeaders(), is(Collections.emptyMap()));
  }

  @Test
  public void shouldCreateTlsClientOptions() {
    // Given:
    final Map<String, String> requestHeaders = ImmutableMap.of("h1", "v1", "h2", "v2");
    final ClientOptions clientOptions = createClientOptions(TLS_URL, "user",
        "pass", "abc", null, null,
        null, null, null, true, true, requestHeaders);

    // Then:
    assertThat(clientOptions.isUseTls(), is(true));
    assertThat(clientOptions.getBasicAuthUsername(), is("user"));
    assertThat(clientOptions.getBasicAuthPassword(), is("pass"));
    assertThat(clientOptions.getTrustStore(), is("abc"));
    assertThat(clientOptions.getTrustStorePassword(), is(""));
    assertThat(clientOptions.getKeyStore(), is(""));
    assertThat(clientOptions.getKeyStorePassword(), is(""));
    assertThat(clientOptions.getKeyPassword(), is(""));
    assertThat(clientOptions.getKeyAlias(), is(""));
    assertThat(clientOptions.isUseAlpn(), is(true));
    assertThat(clientOptions.isVerifyHost(), is(true));
    assertThat(clientOptions.getRequestHeaders(), is(requestHeaders));
  }
}
