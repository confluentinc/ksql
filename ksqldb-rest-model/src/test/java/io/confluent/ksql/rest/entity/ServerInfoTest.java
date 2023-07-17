/*
 * Copyright 2018 Confluent Inc.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.ApiJsonMapper;
import java.io.IOException;
import org.junit.Test;

public class ServerInfoTest {
  private static final String VERSION = "test-version";
  private static final String KAFKA_CLUSTER_ID = "test-kafka-cluster";
  private static final String KSQL_SERVICE_ID = "test-ksql-service";

  private final ServerInfo serverInfo = new ServerInfo(VERSION, KAFKA_CLUSTER_ID, KSQL_SERVICE_ID, "status");

  @Test
  public void testSerializeDeserialize() throws IOException {
    final ObjectMapper mapper = ApiJsonMapper.INSTANCE.get();
    final byte[] bytes = mapper.writeValueAsBytes(serverInfo);
    final ServerInfo deserializedServerInfo = mapper.readValue(bytes, ServerInfo.class);
    assertThat(serverInfo, equalTo(deserializedServerInfo));
  }
}
