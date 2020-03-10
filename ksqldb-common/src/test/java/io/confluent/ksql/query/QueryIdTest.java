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

package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import org.junit.Test;

public class QueryIdTest {

  private final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;

  @Test
  public void shouldSerializeCorrectly() throws Exception {
    // Given:
    final QueryId id = new QueryId("query-id");

    // When:
    final String serialized = objectMapper.writeValueAsString(id);

    assertThat(serialized, is("\"query-id\""));
  }

  @Test
  public void shouldDeserializeCorrectly() throws Exception {
    // Given:
    final String serialized = "\"an-id\"";

    // When:
    final QueryId deserialized = objectMapper.readValue(serialized, QueryId.class);

    // Then:
    assertThat(deserialized, is(new QueryId("an-id")));
  }
}