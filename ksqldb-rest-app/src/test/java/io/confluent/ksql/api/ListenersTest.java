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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Test;

public class ListenersTest extends BaseApiTest {

  private static final ServerKeyStore SERVER_KEY_STORE = new ServerKeyStore();

  @Test
  public void shouldSupportOneListener() {
    //Given:
    init();
    createServer(createConfig("http://localhost:8088", false));
    this.client = createClient();

    // When:
    List<URI> listeners = server.getListeners();

    // Then:
    assertThat(listeners, hasSize(1));
    assertThat(listeners.get(0), is(URI.create("http://localhost:8088")));
  }

  @Test
  public void shouldSupportMultipleListenersSameProtocols() {
    // Given:
    init();
    createServer(createConfig("http://localhost:8088, http://localhost:8089", true));
    this.client = createClient();

    // When:
    List<URI> listeners = server.getListeners();

    // Then:
    assertThat(listeners, hasSize(2));
    assertThat(listeners.get(0), is(URI.create("http://localhost:8088")));
    assertThat(listeners.get(1), is(URI.create("http://localhost:8089")));
  }

  @Test
  public void shouldSupportMultipleListenersDifferentProtocols() {
    // Given:
    init();
    createServer(createConfig("http://localhost:8088, https://localhost:8089", true));
    this.client = createClient();

    // When:
    List<URI> listeners = server.getListeners();

    // Then:
    assertThat(listeners, hasSize(2));
    assertThat(listeners.get(0), is(URI.create("http://localhost:8088")));
    assertThat(listeners.get(1), is(URI.create("https://localhost:8089")));
  }

  @Test
  public void shouldSupportOneProxyProtocolListener() {
    // Given:
    init();
    createServer(createConfig("http://localhost:8088", false, "http://localhost:8088"));
    this.client = createClient();

    // When:
    List<URI> listeners = server.getListeners();
    List<URI> proxyProtocolListeners = server.getProxyProtocolListeners();

    // Then:
    assertThat(listeners, hasSize(1));
    assertThat(listeners.get(0), is(URI.create("http://localhost:8088")));
    assertThat(proxyProtocolListeners, hasSize(1));
    assertThat(proxyProtocolListeners.get(0), is(URI.create("http://localhost:8088")));
  }

  @Test
  public void shouldSupportAdditionalProxyProtocolListenersAndTLS() {
    // Given:
    init();
    createServer(createConfig(
        "http://localhost:8087, http://localhost:8088, https://localhost:8090", true,
        "http://localhost:8088, https://localhost:8090"));
    this.client = createClient();

    // When:
    List<URI> listeners = server.getListeners();
    List<URI> proxyProtocolListeners = server.getProxyProtocolListeners();

    // Then:
    assertThat(listeners, hasSize(3));
    assertThat(listeners.get(0), is(URI.create("http://localhost:8087")));
    assertThat(listeners.get(1), is(URI.create("http://localhost:8088")));
    assertThat(listeners.get(2), is(URI.create("https://localhost:8090")));
    assertThat(proxyProtocolListeners, hasSize(2));
    assertThat(proxyProtocolListeners.get(0), is(URI.create("http://localhost:8088")));
    assertThat(proxyProtocolListeners.get(1), is(URI.create("https://localhost:8090")));
  }

  @Test
  public void shouldFailToStartIfNonExistingListenerInProxyProtocolList() {
    // Given:
    init();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> createServer(createConfig(
            "http://localhost:8089", false,
            "http://localhost:8089, http://localhost:8090"))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Listener http://localhost:8090 is listed in listeners.proxy.protocol"
            + " but not in listeners"));

  }

  @Test
  public void shouldFailToStartIfListenerWithHttpsButNoKeyStore() {
    // Given:
    init();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> createServer(createConfig("https://localhost:8089", false))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "https listener specified but no keystore provided"));

  }

  @Test
  public void shouldFailToStartWithUnsupportedProtocol() {
    // Given:
    init();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> createServer(createConfig("ftp://localhost:8088", false))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid URI scheme should be http or https"));
  }

  @Test
  public void shouldFailToStartWithInvalidURI() {
    // Given:
    init();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> createServer(createConfig("http:: uiqhwduihqwduhi:8989", false))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid listener URI"));
  }
  private static KsqlRestConfig createConfig(String listeners, boolean tls) {
    return createConfig(listeners, tls, "");
  }

  private static KsqlRestConfig createConfig(String listeners, boolean tls,
      String proxyProtocolListeners) {
    Map<String, Object> config = new HashMap<>();
    config.put(KsqlRestConfig.LISTENERS_CONFIG, listeners);
    config.put(KsqlRestConfig.PROXY_PROTOCOL_LISTENERS_CONFIG, proxyProtocolListeners);
    config.put(KsqlRestConfig.VERTICLE_INSTANCES, 4);

    if (tls) {
      String keyStorePath = SERVER_KEY_STORE.keyStoreProps()
          .get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
      String keyStorePassword = SERVER_KEY_STORE.keyStoreProps()
          .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
      String keyPassword = SERVER_KEY_STORE.keyStoreProps()
          .get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
      String keyAlias = SERVER_KEY_STORE.getKeyAlias();

      config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStorePath);
      config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
      config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
      config.put(KsqlRestConfig.KSQL_SSL_KEYSTORE_ALIAS_EXTERNAL_CONFIG, keyAlias);
    }

    return new KsqlRestConfig(config);
  }

  private void init() {
    stopServer();
    stopClient();
  }
}
