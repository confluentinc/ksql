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

import io.confluent.ksql.api.server.ApiServerConfig;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;


public class ApiServerConfigTest {

  @Test
  public void shouldConvertToJsonObject() {
    Map<String, Object> map = new HashMap<>();

    map.put(ApiServerConfig.VERTICLE_INSTANCES, 2);
    map.put(ApiServerConfig.LISTEN_HOST, "foo.com");
    map.put(ApiServerConfig.LISTEN_PORT, 8089);
    map.put(ApiServerConfig.CERT_PATH, "uygugy");
    map.put(ApiServerConfig.KEY_PATH, "ewfwef");

    ApiServerConfig config = new ApiServerConfig(map);

    JsonObject jsonObject = config.toJsonObject();

    assertThat(2, is(jsonObject.getInteger(ApiServerConfig.VERTICLE_INSTANCES)));
    assertThat("foo.com", is(jsonObject.getString(ApiServerConfig.LISTEN_HOST)));
    assertThat(8089, is(jsonObject.getInteger(ApiServerConfig.LISTEN_PORT)));
    assertThat("uygugy", is(jsonObject.getString(ApiServerConfig.CERT_PATH)));
    assertThat("ewfwef", is(jsonObject.getString(ApiServerConfig.KEY_PATH)));
  }

  @Test
  public void shouldCreateFromJsonObject() {
    JsonObject jsonObject = new JsonObject();

    jsonObject.put(ApiServerConfig.VERTICLE_INSTANCES, 2);
    jsonObject.put(ApiServerConfig.LISTEN_HOST, "foo.com");
    jsonObject.put(ApiServerConfig.LISTEN_PORT, 8089);
    jsonObject.put(ApiServerConfig.CERT_PATH, "uygugy");
    jsonObject.put(ApiServerConfig.KEY_PATH, "ewfwef");

    ApiServerConfig config = new ApiServerConfig(jsonObject);

    JsonObject jsonObject2 = config.toJsonObject();
    assertThat(2, is(jsonObject2.getInteger(ApiServerConfig.VERTICLE_INSTANCES)));
    assertThat("foo.com", is(jsonObject2.getString(ApiServerConfig.LISTEN_HOST)));
    assertThat(8089, is(jsonObject2.getInteger(ApiServerConfig.LISTEN_PORT)));
    assertThat("uygugy", is(jsonObject2.getString(ApiServerConfig.CERT_PATH)));
    assertThat("ewfwef", is(jsonObject2.getString(ApiServerConfig.KEY_PATH)));
  }
}
