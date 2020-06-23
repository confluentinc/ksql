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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.ApiJsonMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaTopicsListExtendedTest {

  private static final ObjectMapper MAPPER = ApiJsonMapper.INSTANCE.get();

  @Test
  public void shouldRoundTrip() throws Exception {
    // Given:
    final KafkaTopicsListExtended original = new KafkaTopicsListExtended(
        "SHOW TOPICS EXTENDED;",
        ImmutableList.of(new KafkaTopicInfoExtended(
            "thetopic",
            ImmutableList.of(1, 2, 3),
            19L,
            42,
            12)
        )
    );

    // When:
    final String json = MAPPER.writeValueAsString(original);
    final KafkaTopicsListExtended actual = MAPPER.readValue(json, KafkaTopicsListExtended.class);

    // Then:
    assertThat("serialized wrong", json, is(
        "{"
            + "\"@type\":\"kafka_topics_extended\","
            + "\"statementText\":\"SHOW TOPICS EXTENDED;\","
            + "\"topics\":["
            + "{\"name\":\"thetopic\",\"replicaInfo\":[1,2,3],\"consumerCount\":42,\"consumerGroupCount\":12,\"msgCount\":19}"
            + "],\"warnings\":[]}"
    ));

    assertThat("deserialized wrong", actual, is(original));
  }

  @Test
  public void shouldDeserializePreV11() throws Exception {
    // Given:
    final String jsonWithoutMsgCount = "{"
        + "\"@type\":\"kafka_topics_extended\","
        + "\"statementText\":\"SHOW TOPICS EXTENDED;\","
        + "\"topics\":["
        + "{\"name\":\"t\",\"replicaInfo\":[1,2,3],\"consumerCount\":42,\"consumerGroupCount\":12}"
        + "],\"warnings\":[]}";

    // When:
    final KafkaTopicsListExtended actual = MAPPER
        .readValue(jsonWithoutMsgCount, KafkaTopicsListExtended.class);

    // Then:
    assertThat(actual, is(new KafkaTopicsListExtended(
        "SHOW TOPICS EXTENDED;",
        ImmutableList.of(new KafkaTopicInfoExtended(
            "t",
            ImmutableList.of(1, 2, 3),
            0L,
            42,
            12)
        )
    )));
  }
}
