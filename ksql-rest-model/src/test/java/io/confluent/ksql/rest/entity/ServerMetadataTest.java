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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ServerMetadataTest {
  private static final ObjectMapper OBJECT_MAPPER = JsonMapper.INSTANCE.mapper;

  @Test
  public void shouldReturnServerMetadata() throws IOException {
    // Given:
    final ServerMetadata expected = new ServerMetadata(
        "1.0.0",
        ServerClusterId.of("kafka1", "ksql1")
    );

    // When:
    final String json = OBJECT_MAPPER.writeValueAsString(expected);
    final ServerMetadata actual = OBJECT_MAPPER.readValue(json, ServerMetadata.class);

    // Then:
    assertEquals(
        "{" +
            "\"version\":\"1.0.0\"," +
            "\"clusterId\":" +
            "{\"id\":\"\"," +
            "\"scope\":" +
            "{\"kafka-cluster\":\"kafka1\",\"ksql-cluster\":\"ksql1\"}}}",
        json);

    assertEquals(expected, actual);
  }
}
