/*
 * Copyright 2024 Confluent Inc.
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

package io.confluent.ksql.security.oauth;

import io.confluent.ksql.security.KsqlClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StaticTokenCredentialsTest {

  private StaticTokenCredentials credentials;

  @BeforeEach
  void setUp() {
    credentials = new StaticTokenCredentials();
  }

  @Test
  void testGetAuthHeaderWithValidToken() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KsqlClientConfig.BEARER_AUTH_TOKEN_CONFIG, "validToken");

    credentials.configure(configs);

    String expectedHeader = "Bearer validToken";
    assertEquals(expectedHeader, credentials.getAuthHeader());
  }

  @Test
  void testValidateConfigsThrowsExceptionForMissingToken() {
    Map<String, Object> configs = new HashMap<>();

    ConfigException thrown = assertThrows(
        ConfigException.class,
        () -> credentials.validateConfigs(configs),
        "Expected validateConfigs to throw, but it didn't"
    );

    assertTrue(thrown.getMessage()
        .contains("Cannot configure StaticTokenCredentials without a proper token."));
  }

  @Test
  void testConfigureWithValidToken() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KsqlClientConfig.BEARER_AUTH_TOKEN_CONFIG, "anotherValidToken");

    assertDoesNotThrow(() -> credentials.configure(configs));
    assertEquals("Bearer anotherValidToken", credentials.getAuthHeader());
  }
}
