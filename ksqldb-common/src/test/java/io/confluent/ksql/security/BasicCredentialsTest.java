/*
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.security;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BasicCredentialsTest {

  @Test
  public void testOfCreatesInstance() {
    BasicCredentials credentials = BasicCredentials.of("user", "pass");
    assertNotNull(credentials);
  }

  @Test
  public void testUsernameAndPasswordMethods() {
    BasicCredentials credentials = BasicCredentials.of("user", "pass");
    assertEquals("user", credentials.username());
    assertEquals("pass", credentials.password());
  }

  @Test
  public void testEqualsMethod() {
    BasicCredentials credentials1 = BasicCredentials.of("user", "pass");
    BasicCredentials credentials2 = BasicCredentials.of("user", "pass");
    BasicCredentials credentials3 = BasicCredentials.of("user2", "pass2");

    assertEquals(credentials1, credentials2);
    assertNotEquals(credentials1, credentials3);
  }

  @Test
  public void testHashCodeMethod() {
    BasicCredentials credentials1 = BasicCredentials.of("user", "pass");
    BasicCredentials credentials2 = BasicCredentials.of("user", "pass");

    assertEquals(credentials1.hashCode(), credentials2.hashCode());
  }

  @Test
  public void testGetAuthHeader() {
    BasicCredentials credentials = BasicCredentials.of("user", "pass");
    String expectedAuthHeader = "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8));

    assertEquals(expectedAuthHeader, credentials.getAuthHeader());
  }

  @Test
  public void testValidation() {
    assertThrows(ConfigException.class, () -> BasicCredentials.of("", "pass"));
  }
}
