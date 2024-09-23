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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;

public class IdpConfigTest {

  @Test
  public void testBuilder() {
    final String url = "https://idp.example.com/token";
    final String clientId = "client-123";
    final String clientSecret = "secret-456";
    final String scope = "openid profile email";
    final String scopeClaimName = "scp";
    final String subClaimName = "user";
    final Short cacheExpiryBufferSeconds = 600;

    IdpConfig config = new IdpConfig.Builder()
        .withTokenEndpointUrl(url)
        .withClientId(clientId)
        .withClientSecret(clientSecret)
        .withScope(scope)
        .withScopeClaimName(scopeClaimName)
        .withSubClaimName(subClaimName)
        .withCacheExpiryBufferSeconds(cacheExpiryBufferSeconds)
        .build();

    assertEquals(url, config.getIdpTokenEndpointUrl());
    assertEquals(clientId, config.getIdpClientId());
    assertEquals(clientSecret, config.getIdpClientSecret());
    assertEquals(scope, config.getIdpScope());
    assertEquals(scopeClaimName, config.getIdpScopeClaimName());
    assertEquals(subClaimName, config.getIdpSubClaimName());
    assertEquals(cacheExpiryBufferSeconds, config.getIdpCacheExpiryBufferSeconds());
  }

  @Test
  public void testDefaultsWithBuilder() {
    final String url = "https://idp.example.com/token";
    final String clientId = "client-123";
    final String clientSecret = "secret-456";

    IdpConfig config = new IdpConfig.Builder()
        .withTokenEndpointUrl(url)
        .withClientId(clientId)
        .withClientSecret(clientSecret)
        .build();

    assertEquals(url, config.getIdpTokenEndpointUrl());
    assertEquals(clientId, config.getIdpClientId());
    assertEquals(clientSecret, config.getIdpClientSecret());
    assertEquals("", config.getIdpScope());
    assertEquals("scope", config.getIdpScopeClaimName());
    assertEquals("sub", config.getIdpSubClaimName());
    assertEquals(Short.valueOf((short) 300), config.getIdpCacheExpiryBufferSeconds());
  }

  @Test
  public void testCopy() {
    IdpConfig originalConfig = new IdpConfig.Builder()
        .withTokenEndpointUrl("https://idp.example.com/token")
        .withClientId("client-123")
        .withClientSecret("secret-456")
        .withScope("openid profile email")
        .withScopeClaimName("scp")
        .withSubClaimName("user")
        .withCacheExpiryBufferSeconds((short) 600)
        .build();

    IdpConfig copiedConfig = originalConfig.copy();

    // Confirm that all fields are equal between the original and the copy.
    assertEquals(originalConfig.getIdpTokenEndpointUrl(), copiedConfig.getIdpTokenEndpointUrl());
    assertEquals(originalConfig.getIdpClientId(), copiedConfig.getIdpClientId());
    assertEquals(originalConfig.getIdpClientSecret(), copiedConfig.getIdpClientSecret());
    assertEquals(originalConfig.getIdpScope(), copiedConfig.getIdpScope());
    assertEquals(originalConfig.getIdpScopeClaimName(), copiedConfig.getIdpScopeClaimName());
    assertEquals(originalConfig.getIdpSubClaimName(), copiedConfig.getIdpSubClaimName());
    assertEquals(originalConfig.getIdpCacheExpiryBufferSeconds(),
        copiedConfig.getIdpCacheExpiryBufferSeconds());

    // Confirm the two objects are not the same instance.
    assertNotSame(originalConfig, copiedConfig);
  }
}