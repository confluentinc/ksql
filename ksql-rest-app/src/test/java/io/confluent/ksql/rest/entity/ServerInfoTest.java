/**
 * Copyright 2018 Confluent Inc.
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
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

public class ServerInfoTest {
  private static final String VERSION = "test-version";
  private static final String KAFKA_CLUSTER_ID = "test-kafka-cluster";
  private static final String KSQL_SERVICE_ID = "test-ksql-service";

  private ServerInfo serverInfo = new ServerInfo(VERSION, KAFKA_CLUSTER_ID, KSQL_SERVICE_ID);

  @Test
  public void testSerializeDeserialize() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    byte[] bytes = mapper.writeValueAsBytes(serverInfo);
    ServerInfo deserializedServerInfo = mapper.readValue(bytes, ServerInfo.class);
    Assert.assertThat(serverInfo, equalTo(deserializedServerInfo));
  }
}
