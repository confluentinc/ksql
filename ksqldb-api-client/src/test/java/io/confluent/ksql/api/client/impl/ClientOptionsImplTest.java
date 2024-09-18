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

package io.confluent.ksql.api.client.impl;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.api.client.ClientOptions;
import java.util.Collections;

import io.confluent.ksql.security.oauth.IdpConfig;
import org.junit.Test;

public class ClientOptionsImplTest {

  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            ClientOptions.create(),
            ClientOptions.create()
                .setHost(ClientOptions.DEFAULT_HOST)
                .setPort(ClientOptions.DEFAULT_HOST_PORT)
        )
        .addEqualityGroup(
            ClientOptions.create().setHost("foo")
        )
        .addEqualityGroup(
            ClientOptions.create().setPort(443)
        )
        .addEqualityGroup(
            ClientOptions.create().setUseTls(true)
        )
        .addEqualityGroup(
            ClientOptions.create().setVerifyHost(false)
        )
        .addEqualityGroup(
            ClientOptions.create().setUseAlpn(true)
        )
        .addEqualityGroup(
            ClientOptions.create().setTrustStore("trust_store")
        )
        .addEqualityGroup(
            ClientOptions.create().setTrustStorePassword("trust_store_pass")
        )
        .addEqualityGroup(
            ClientOptions.create().setKeyStore("key_store")
        )
        .addEqualityGroup(
            ClientOptions.create().setKeyStorePassword("key_store_pass")
        )
        .addEqualityGroup(
            ClientOptions.create().setBasicAuthCredentials("user", "pass")
        )
        .addEqualityGroup(
            ClientOptions.create().setIdpConfig(new IdpConfig.Builder()
                .withTokenEndpointUrl("http://localhost:8080")
                .withClientId("user")
                .withClientSecret("pass")
                .withScope("all")
                .withScopeClaimName("newScope")
                .withSubClaimName("newSub")
                .withCacheExpiryBufferSeconds((short) 500)
                .build())
        )
        .addEqualityGroup(
            ClientOptions.create().setExecuteQueryMaxResultRows(10)
        )
        .addEqualityGroup(
            ClientOptions.create().setHttp2MultiplexingLimit(5)
        )
        .addEqualityGroup(
            ClientOptions.create().setRequestHeaders(Collections.singletonMap("h1", "v1"))
        )
        .testEquals();
  }

}
