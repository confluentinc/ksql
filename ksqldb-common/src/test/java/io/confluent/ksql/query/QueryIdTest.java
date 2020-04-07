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
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.json.JsonMapper;
import org.junit.Test;

public class QueryIdTest {

  private final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(new QueryId("matching"), new QueryId("MaTcHiNg"))
        .addEqualityGroup(new QueryId("different"))
        .testEquals();
  }

  @Test
  public void shouldBeCaseInsensitiveOnCommparison() {
    // When:
    final QueryId id = new QueryId("Mixed-Case-Id");

    // Then:
    assertThat(id, is(new QueryId("MIXED-CASE-ID")));
  }

  @Test
  public void shouldPreserveCase() {
    // When:
    final QueryId id = new QueryId("Mixed-Case-Id");

    // Then:
    assertThat(id.toString(), is("Mixed-Case-Id"));
  }

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
