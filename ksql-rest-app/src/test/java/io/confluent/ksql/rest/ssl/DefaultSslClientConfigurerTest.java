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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.test.util.secure.ClientTrustStore;
import io.confluent.ksql.test.util.secure.ServerKeyStore;
import io.confluent.rest.RestConfig;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.ClientBuilder;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSslClientConfigurerTest {

  @Mock
  private ClientBuilder clientBuilder;
  private Map<String, String> clientProps;
  private DefaultSslClientConfigurer configurer;

  @Before
  public void setUp() {
    clientProps = new HashMap<>();
    configurer = new DefaultSslClientConfigurer();
  }

  @Test
  public void shouldConfigureKeyStoreIfLocationSet() {
    // Given:
    clientProps.putAll(ServerKeyStore.keyStoreProps());
    final String keyPassword = clientProps.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);

    // When:
    configurer.configureSsl(clientBuilder, clientProps);

    // Then:
    verify(clientBuilder).keyStore(any(), eq(keyPassword));
  }

  @Test
  public void shouldConfigureTrustStoreIfLocationSet() {
    // Given:
    clientProps.putAll(ClientTrustStore.trustStoreProps());

    // When:
    configurer.configureSsl(clientBuilder, clientProps);

    // Then:
    verify(clientBuilder).trustStore(any());
  }

  @Test
  public void shouldConfigureHostNameVerifierSet() {
    // Given:
    clientProps.put(RestConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

    // When:
    configurer.configureSsl(clientBuilder, clientProps);

    // Then:
    verify(clientBuilder).hostnameVerifier(any());
  }
}