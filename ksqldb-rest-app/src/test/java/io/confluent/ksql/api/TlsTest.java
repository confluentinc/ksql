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

package io.confluent.ksql.api;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlsTest extends ApiTest {

  protected static final Logger log = LoggerFactory.getLogger(TlsTest.class);

  @Override
  protected KsqlRestConfig createServerConfig() {
    String keyStorePath = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    String keyStorePassword = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    String trustStorePath = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    String trustStorePassword = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

    Map<String, Object> config = new HashMap<>();
    config.put(KsqlRestConfig.LISTENERS_CONFIG, "https://localhost:0");
    config.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2");
    config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath);
    config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
    config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath);
    config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
    config.put(KsqlRestConfig.VERTICLE_INSTANCES, 4);

    config.put(KsqlRestConfig.SSL_KEYSTORE_RELOAD_CONFIG, true);

    return new KsqlRestConfig(config);
  }

  @Override
  protected WebClientOptions createClientOptions() {
    // for this test file, the client must use a different trust store location than the server
    // since the client store should always be valid even when the server store is loaded with an
    // invalid cert
    String clientTrustStorePath = ServerKeyStore.clientKeyStoreProps()
        .get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    String clientTrustStorePassword = ServerKeyStore.clientKeyStoreProps()
        .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

    return new WebClientOptions().setSsl(true).
        setUseAlpn(true).
        setProtocolVersion(HttpVersion.HTTP_2).
        setTrustStoreOptions(
            new JksOptions().setPath(clientTrustStorePath).setPassword(clientTrustStorePassword)).
        setVerifyHost(false).
        setDefaultHost("localhost").
        setDefaultPort(server.getListeners().get(0).getPort());
  }

  @Test
  public void shouldFailToUseDisabledTlsVersion() {
    // Given
    WebClientOptions clientOptions = createClientOptions()
        .setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.1"));
    WebClient client = WebClient.create(vertx, clientOptions);

    // When
    JsonObject requestBody = new JsonObject().put("ksql", "show streams;");
    VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
    client
        .post("/ksql")
        .sendBuffer(requestBody.toBuffer(), requestFuture);

    // Then
    try {
      requestFuture.get();
    } catch (Exception e) {
      assertThat(e,
          instanceOf(ExecutionException.class)); // thrown from CompletableFuture.get()
      assertThat(e.getMessage(), containsString(
          "javax.net.ssl.SSLHandshakeException: Failed to create SSL connection"));
    }
  }

  @Ignore
  @Test
  public void shouldReloadCert() throws Exception {
    JsonObject requestBody = new JsonObject().put("sql", DEFAULT_PULL_QUERY);

    // Given: sanity check that a query succeeds
    HttpResponse<Buffer> response = sendPostRequest("/query-stream", requestBody.toBuffer());
    assertThat(response.statusCode(), is(200));
    assertThat(response.statusMessage(), is("OK"));

    try {
      // When: load expired key store
      ServerKeyStore.loadExpiredServerKeyStore();
      assertThatEventually(
          "Should fail to execute query with expired key store",
          () -> {
            // re-create client since server port changes on restart
            this.client = createClient();

            try {
              // this should fail
              sendPostRequest("/query-stream", requestBody.toBuffer());
              return "error: request should have failed but did not";
            } catch (Exception e) {
              assertThat(e,
                  instanceOf(ExecutionException.class)); // thrown from CompletableFuture.get()
              return e.getMessage();
            }
          },
          containsString("javax.net.ssl.SSLHandshakeException: Failed to create SSL connection"),
          TimeUnit.SECONDS.toMillis(1),
          TimeUnit.SECONDS.toMillis(1)
      );
    } finally { // restore cert regardless of failure above so as to not affect other tests
      // When: load valid store
      ServerKeyStore.loadValidServerKeyStore();
      assertThatEventually(
          "Should successfully execute query with valid key store",
          () -> {
            // re-create client since server port changes on restart
            this.client = createClient();

            try {
              return sendPostRequest("/query-stream", requestBody.toBuffer()).statusCode();
            } catch (Exception e) {
              return 0;
            }
          },
          is(200),
          TimeUnit.SECONDS.toMillis(1),
          TimeUnit.SECONDS.toMillis(1)
      );
    }
  }
}
