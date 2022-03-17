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

package io.confluent.ksql.api.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Test;

public class ClientTlsTest extends ClientTest {

  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  protected static final String TRUST_STORE_PATH = SERVER_KEY_STORE.keyStoreProps()
      .get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
  protected static final String TRUST_STORE_PASSWORD = SERVER_KEY_STORE.keyStoreProps()
      .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
  protected static final String KEY_STORE_PATH = SERVER_KEY_STORE.keyStoreProps()
      .get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
  protected static final String KEY_STORE_PASSWORD = SERVER_KEY_STORE.keyStoreProps()
      .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
  protected static final String KEY_PASSWORD = SERVER_KEY_STORE.keyStoreProps()
      .get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
  protected static final String KEYSTORE_ALIAS = SERVER_KEY_STORE.getKeyAlias();

  @Override
  protected KsqlRestConfig createServerConfig() {
    KsqlRestConfig config = super.createServerConfig();
    Map<String, Object> origs = config.originals();
    origs.put(KsqlRestConfig.LISTENERS_CONFIG, "https://localhost:0");
    origs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KEY_STORE_PATH);
    origs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEY_STORE_PASSWORD);
    origs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD);
    origs.put(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_INTERNAL_CONFIG, KEYSTORE_ALIAS);
    origs.put(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG, KEYSTORE_ALIAS);
    return new KsqlRestConfig(origs);
  }

  @Override
  protected ClientOptions createJavaClientOptions() {
    return super.createJavaClientOptions()
        .setUseTls(true)
        .setTrustStore(TRUST_STORE_PATH)
        .setTrustStorePassword(TRUST_STORE_PASSWORD)
        .setVerifyHost(false)
        .setUseAlpn(true);
  }

  @Test
  public void shouldFailRequestIfServerNotInTrustStore() {
    // Given
    final Client client = Client.create(clientOptionsWithoutTrustStore(), vertx);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally,
        () -> client.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(SSLHandshakeException.class));
    assertThat(e.getCause().getMessage(), containsString("Failed to create SSL connection"));
  }

  private ClientOptions clientOptionsWithoutTrustStore() {
    return ClientOptions.create()
        .setHost("localhost")
        .setPort(server.getListeners().get(0).getPort())
        .setUseTls(true)
        .setVerifyHost(false)
        .setUseAlpn(true);
  }
}
