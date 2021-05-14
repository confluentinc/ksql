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

package io.confluent.ksql.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.query.QueryId;
import org.junit.Test;

public class QueryIdSerializationTest {

  private final ObjectMapper objectMapper = ApiJsonMapper.INSTANCE.get();

  @Test
  public void shouldSerializeMaintainingCase() throws Exception {
    // Given:
    final QueryId id = new QueryId("Query-Id");

    // When:
    final String serialized = objectMapper.writeValueAsString(id);

    assertThat(serialized, is("\"Query-Id\""));
  }

  @Test
  public void shouldDeserializeMaintainingCase() throws Exception {
    // Given:
    final String serialized = "\"An-Id\"";

    // When:
    final QueryId deserialized = objectMapper.readValue(serialized, QueryId.class);

    // Then:
    assertThat(deserialized.toString(), is("An-Id"));
  }
}
