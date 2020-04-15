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
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

@SuppressWarnings("SameParameterValue")
public class QueryStatusCountTest {

  private QueryStatusCount queryStatusCount;
  
  @Before
  public void setup() {
    queryStatusCount = new QueryStatusCount();
  }

  @Test
  public void shouldUpdateExistingStatusCount() {
    queryStatusCount.updateStatusCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStatusCount.getStatuses().get(KsqlQueryStatus.RUNNING),
        is(2));

    queryStatusCount.updateStatusCount(KsqlQueryStatus.RUNNING, 4);
    assertThat(
        queryStatusCount.getStatuses().get(KsqlQueryStatus.RUNNING),
        is(6));

    queryStatusCount.updateStatusCount(KafkaStreams.State.CREATED, 2);
    assertThat(
        queryStatusCount.getStatuses().get(KsqlQueryStatus.RUNNING),
        is(8));

    queryStatusCount.updateStatusCount(KafkaStreams.State.ERROR, 1);
    assertThat(
        queryStatusCount.getStatuses().get(KsqlQueryStatus.ERROR),
        is(1));

    queryStatusCount.updateStatusCount(KsqlQueryStatus.ERROR, 3);
    assertThat(
        queryStatusCount.getStatuses().get(KsqlQueryStatus.ERROR),
        is(4));
  }

  @Test
  public void shouldReturnAggregateStatus() {
    queryStatusCount.updateStatusCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStatusCount.getAggregateStatus(),
        is(KsqlQueryStatus.RUNNING));
    queryStatusCount.updateStatusCount(KafkaStreams.State.ERROR, 1);
    assertThat(
        queryStatusCount.getAggregateStatus(),
        is(KsqlQueryStatus.ERROR));
  }

  @Test
  public void shouldToString() {
    queryStatusCount.updateStatusCount(KafkaStreams.State.RUNNING, 2);
    assertThat(
        queryStatusCount.toString(),
        is("RUNNING:2"));
    queryStatusCount.updateStatusCount(KafkaStreams.State.NOT_RUNNING, 1);
    assertThat(
        queryStatusCount.toString(),
        is("RUNNING:3"));
  }

  @Test
  public void shouldImplementHashCodeAndEqualsCorrectly() {
    final QueryStatusCount queryStatusCount1 = new QueryStatusCount(Collections.singletonMap(KsqlQueryStatus.ERROR, 2));
    final QueryStatusCount queryStatusCount2 = new QueryStatusCount(Collections.singletonMap(KsqlQueryStatus.RUNNING, 1));
    queryStatusCount2.updateStatusCount(KafkaStreams.State.ERROR, 2);
    final QueryStatusCount queryStatusCount3 = new QueryStatusCount();
    queryStatusCount3.updateStatusCount(KafkaStreams.State.ERROR, 2);

    new EqualsTester()
        .addEqualityGroup(queryStatusCount, queryStatusCount)
        .addEqualityGroup(queryStatusCount1, queryStatusCount3)
        .addEqualityGroup(queryStatusCount2)
        .testEquals();
  }

  @Test
  public void shouldRoundTripWhenEmpty() {
    // When:
    final String json = assertDeserializedToSame(queryStatusCount);

    // Then:
    assertThat(json, is("{}"));
  }

  @Test
  public void shouldRoundTripWhenNotEmpty() {
    // Given:
    queryStatusCount.updateStatusCount(KafkaStreams.State.RUNNING, 2);
    queryStatusCount.updateStatusCount(KafkaStreams.State.ERROR, 10);
    queryStatusCount.updateStatusCount(KsqlQueryStatus.UNRESPONSIVE, 1);

    // When:
    final String json = assertDeserializedToSame(queryStatusCount);

    // Then:
    assertThat(json, is("{"
        + "\"RUNNING\":2,"
        + "\"ERROR\":10,"
        + "\"UNRESPONSIVE\":1"
        + "}"));
  }

  private static String assertDeserializedToSame(final QueryStatusCount original) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      final String json = mapper.writeValueAsString(original);

      final QueryStatusCount deserialized = mapper
          .readValue(json, QueryStatusCount.class);

      assertThat(deserialized, is(original));

      return json;
    } catch (Exception e) {
      throw new AssertionError("Failed to round trip", e);
    }
  }
}