/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.ServerClusterId.Scope;
import org.junit.Test;

public class ServerClusterIdTest {

  private static final ObjectMapper OBJECT_MAPPER = ApiJsonMapper.INSTANCE.get();

  private static final String JSON = "{"
      + "\"scope\":{"
      + "\"path\":[\"p1\",\"p2\"],"
      + "\"clusters\":{\"kafka-cluster\":\"kafka1\",\"ksql-cluster\":\"ksql1\"}"
      + "},"
      + "\"id\":\"\""
      + "}";

  private static final ServerClusterId CLUSTER_ID = new ServerClusterId(new Scope(
      ImmutableList.of("p1", "p2"),
      ImmutableMap.of(
          "kafka-cluster", "kafka1",
          "ksql-cluster", "ksql1"
      )
  ));

  @Test
  public void shouldSerializeToJson() throws Exception {
    assertThat(OBJECT_MAPPER.writeValueAsString(CLUSTER_ID), is(JSON));
  }

  @Test
  public void shouldDeserializeFromJson() throws Exception {
    assertThat(OBJECT_MAPPER.readValue(JSON, ServerClusterId.class), is(CLUSTER_ID));
  }
}
