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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryState;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

@SuppressWarnings("SameParameterValue")
public class QueryStateCountTest {

  QueryStateCount queryStateCount;
  
  @Before
  public void setup() {
    queryStateCount = new QueryStateCount();
  }

  @Test
  public void shouldUpdateExistingStateCount() {
    queryStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStateCount.getStates().get(KsqlQueryState.RUNNING),
        is(2));

    queryStateCount.updateStateCount(KsqlQueryState.RUNNING, 4);
    assertThat(
        queryStateCount.getStates().get(KsqlQueryState.RUNNING),
        is(6));

    queryStateCount.updateStateCount(KafkaStreams.State.CREATED, 2);
    assertThat(
        queryStateCount.getStates().get(KsqlQueryState.RUNNING),
        is(8));

    queryStateCount.updateStateCount(KafkaStreams.State.ERROR, 1);
    assertThat(
        queryStateCount.getStates().get(KsqlQueryState.ERROR),
        is(1));

    queryStateCount.updateStateCount(KsqlQueryState.ERROR, 3);
    assertThat(
        queryStateCount.getStates().get(KsqlQueryState.ERROR),
        is(4));
  }

  @Test
  public void shouldToString() {
    queryStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStateCount.toString(),
        is("RUNNING:2"));
    queryStateCount.updateStateCount(KafkaStreams.State.NOT_RUNNING, 1);
    assertThat(
        queryStateCount.toString(),
        is("RUNNING:3"));
  }

  @Test
  public void shouldImplementHashCodeAndEqualsCorrectly() {
    final QueryStateCount queryStateCount1 = new QueryStateCount(Collections.singletonMap(KafkaStreams.State.ERROR, 2));
    final QueryStateCount queryStateCount2 = new QueryStateCount(Collections.singletonMap(KafkaStreams.State.RUNNING, 1));
    queryStateCount2.updateStateCount(KafkaStreams.State.ERROR, 2);
    final QueryStateCount queryStateCount3 = new QueryStateCount();
    queryStateCount3.updateStateCount(KafkaStreams.State.ERROR, 2);

    new EqualsTester()
        .addEqualityGroup(queryStateCount, queryStateCount)
        .addEqualityGroup(queryStateCount1, queryStateCount3)
        .addEqualityGroup(queryStateCount2)
        .testEquals();
  }

  @Test
  public void shouldRoundTripWhenEmpty() {
    // When:
    final String json = assertDeserializedToSame(queryStateCount);

    // Then:
    assertThat(json, is("{}"));
  }

  @Test
  public void shouldRoundTripWhenNotEmpty() {
    // Given:
    queryStateCount.updateStateCount(KafkaStreams.State.RUNNING, 2);
    queryStateCount.updateStateCount(KafkaStreams.State.ERROR, 10);

    // When:
    final String json = assertDeserializedToSame(queryStateCount);

    // Then:
    assertThat(json, is("{"
        + "\"RUNNING\":2,"
        + "\"ERROR\":10"
        + "}"));
  }

  private static String assertDeserializedToSame(final QueryStateCount original) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      final String json = mapper.writeValueAsString(original);

      final QueryStateCount deserialized = mapper
          .readValue(json, QueryStateCount.class);

      assertThat(deserialized, is(original));

      return json;
    } catch (Exception e) {
      throw new AssertionError("Failed to round trip", e);
    }
  }
}