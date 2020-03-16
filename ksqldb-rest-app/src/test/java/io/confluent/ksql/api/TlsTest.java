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

import io.confluent.ksql.api.auth.ApiServerConfig;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TlsTest extends ApiTest {

  protected static final Logger log = LoggerFactory.getLogger(TlsTest.class);

  @Override
  protected ApiServerConfig createServerConfig() {

    String keyStorePath = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    String keyStorePassword = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    String trustStorePath = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    String trustStorePassword = ServerKeyStore.keyStoreProps()
        .get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);

    Map<String, Object> config = new HashMap<>();
    config.put(ApiServerConfig.LISTENERS, "https://localhost:0");
    config.put(ApiServerConfig.TLS_KEY_STORE_PATH, keyStorePath);
    config.put(ApiServerConfig.TLS_KEY_STORE_PASSWORD, keyStorePassword);
    config.put(ApiServerConfig.TLS_TRUST_STORE_PATH, trustStorePath);
    config.put(ApiServerConfig.TLS_TRUST_STORE_PASSWORD, trustStorePassword);
    config.put(ApiServerConfig.VERTICLE_INSTANCES, 4);

    return new ApiServerConfig(config);
  }

  @Override
  protected WebClientOptions createClientOptions() {
    return new WebClientOptions().setSsl(true).
        setUseAlpn(true).
        setProtocolVersion(HttpVersion.HTTP_2).
        setTrustAll(true).
        setVerifyHost(false).
        setDefaultHost("localhost").
        setDefaultPort(server.getListeners().get(0).getPort());
  }

}
