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
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.server.ApiServerConfig;
import io.vertx.core.http.ClientAuth;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class ApiServerConfigTest {

  @Test
  public void shouldCreateFromMap() {

    // Given:
    Map<String, Object> map = new HashMap<>();
    map.put(ApiServerConfig.VERTICLE_INSTANCES, 2);
    map.put(ApiServerConfig.LISTEN_HOST, "foo.com");
    map.put(ApiServerConfig.LISTEN_PORT, 8089);
    map.put(ApiServerConfig.TLS_ENABLED, true);
    map.put(ApiServerConfig.TLS_KEY_STORE_PATH, "uygugy");
    map.put(ApiServerConfig.TLS_KEY_STORE_PASSWORD, "ewfwef");
    map.put(ApiServerConfig.TLS_TRUST_STORE_PATH, "wefewf");
    map.put(ApiServerConfig.TLS_TRUST_STORE_PASSWORD, "ergerg");
    map.put(ApiServerConfig.TLS_CLIENT_AUTH_REQUIRED, "request");

    // When:
    ApiServerConfig config = new ApiServerConfig(map);

    // Then:
    assertThat(config.getInt(ApiServerConfig.VERTICLE_INSTANCES), is(2));
    assertThat(config.getString(ApiServerConfig.LISTEN_HOST), is("foo.com"));
    assertThat(config.getInt(ApiServerConfig.LISTEN_PORT), is(8089));
    assertThat(config.getBoolean(ApiServerConfig.TLS_ENABLED), is(true));
    assertThat(config.getString(ApiServerConfig.TLS_KEY_STORE_PATH), is("uygugy"));
    assertThat(config.getString(ApiServerConfig.TLS_KEY_STORE_PASSWORD), is("ewfwef"));
    assertThat(config.getString(ApiServerConfig.TLS_TRUST_STORE_PATH), is("wefewf"));
    assertThat(config.getString(ApiServerConfig.TLS_TRUST_STORE_PASSWORD), is("ergerg"));
    assertThat(config.getString(ApiServerConfig.TLS_CLIENT_AUTH_REQUIRED), is("request"));
  }

  @Test
  public void shouldUseDefaults() {

    // Given:
    Map<String, Object> map = new HashMap<>();

    // When:
    ApiServerConfig config = new ApiServerConfig(map);

    // Then:
    int expectedVerticleInstances = 2 * Runtime.getRuntime().availableProcessors();
    assertThat(config.getInt(ApiServerConfig.VERTICLE_INSTANCES), is(expectedVerticleInstances));
    assertThat(config.getString(ApiServerConfig.LISTEN_HOST), is("localhost"));
    assertThat(config.getInt(ApiServerConfig.LISTEN_PORT), is(8088));
    assertThat(config.getBoolean(ApiServerConfig.TLS_ENABLED), is(false));
    assertThat(config.getString(ApiServerConfig.TLS_KEY_STORE_PATH), is(""));
    assertThat(config.getString(ApiServerConfig.TLS_KEY_STORE_PASSWORD), is(""));
    assertThat(config.getString(ApiServerConfig.TLS_TRUST_STORE_PATH), is(""));
    assertThat(config.getString(ApiServerConfig.TLS_TRUST_STORE_PASSWORD), is(""));
    assertThat(config.getString(ApiServerConfig.TLS_CLIENT_AUTH_REQUIRED), is("none"));
  }

  @Test(expected = ConfigException.class)
  public void shouldThrowOnUnknownClientAuth() {
    new ApiServerConfig(ImmutableMap.of(
        ApiServerConfig.TLS_CLIENT_AUTH_REQUIRED, "blah"
    ));
  }

  @Test
  public void shouldGetValidClientAuth() {
    // Given:
    final ApiServerConfig config = new ApiServerConfig(ImmutableMap.of(
        ApiServerConfig.TLS_CLIENT_AUTH_REQUIRED, "ReQuIrEd"
    ));

    // Then:
    assertThat(config.getClientAuth(), is(ClientAuth.REQUIRED));
  }
}
