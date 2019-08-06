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

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.json.JsonMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaTopicsListExtendedTest {

  @Test
  public void testSerde() throws Exception {
    // Given:
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final KafkaTopicsListExtended expected = new KafkaTopicsListExtended(
        "SHOW TOPICS EXTENDED;",
        ImmutableList.of(new KafkaTopicInfoExtended("thetopic", ImmutableList.of(1, 2, 3), 42, 12))
    );

    // When:
    final String json = mapper.writeValueAsString(expected);
    final KafkaTopicsListExtended actual = mapper.readValue(json, KafkaTopicsListExtended.class);

    // Then:
    assertEquals(
        "{"
            + "\"@type\":\"kafka_topics_extended\","
            + "\"statementText\":\"SHOW TOPICS EXTENDED;\","
            + "\"topics\":["
            + "{\"name\":\"thetopic\",\"replicaInfo\":[1,2,3],\"consumerCount\":42,\"consumerGroupCount\":12}"
            + "],\"warnings\":[]}",
        json);
    assertEquals(expected, actual);
  }
}
