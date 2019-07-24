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

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.util.KafkaConsumerGroupClient;
import io.confluent.ksql.util.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

public class KafkaTopicsListTest {


  @Test
  public void shouldBuildValidTopicList() {

    // represent the full list of topics
    final Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    final TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(1, new Node(1, "", 8088),
                                                                   Collections.emptyList(), Collections.emptyList());
    topicDescriptions.put("test-topic", new TopicDescription("test-topic", false, Collections.singletonList(topicPartitionInfo)));


    final TopicPartition topicPartition = new TopicPartition("test-topic", 1);
    final KafkaConsumerGroupClientImpl.ConsumerSummary consumerSummary = new KafkaConsumerGroupClientImpl.ConsumerSummary("consumer-id");
    consumerSummary.addPartition(topicPartition);
    final KafkaConsumerGroupClientImpl.ConsumerGroupSummary consumerGroupSummary
        = new KafkaConsumerGroupClientImpl.ConsumerGroupSummary(Collections.singleton(consumerSummary));




    final KafkaConsumerGroupClient consumerGroupClient = mock(KafkaConsumerGroupClient.class);
    expect(consumerGroupClient.listGroups()).andReturn(Collections.singletonList("test-topic"));
    expect(consumerGroupClient.describeConsumerGroup("test-topic")).andReturn(consumerGroupSummary);
    replay(consumerGroupClient);

    final KafkaTopicsList topicsList = KafkaTopicsList.build("statement test", topicDescriptions, new KsqlConfig(Collections.EMPTY_MAP), consumerGroupClient);

    assertThat(topicsList.getTopics().size(), equalTo(1));
    final KafkaTopicInfo first = topicsList.getTopics().iterator().next();
    assertThat(first.getConsumerGroupCount(), equalTo(1));
    assertThat(first.getConsumerCount(), equalTo(1));
    assertThat(first.getReplicaInfo().size(), equalTo(1));

  }

  @Test
  public void testSerde() throws Exception {
    final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
    final KafkaTopicsList expected = new KafkaTopicsList(
        "SHOW TOPICS;",
        ImmutableList.of(new KafkaTopicInfo("thetopic", ImmutableList.of(1, 2, 3), 42, 12))
    );
    final String json = mapper.writeValueAsString(expected);
    assertEquals(
        "{"
            + "\"@type\":\"kafka_topics\","
            + "\"statementText\":\"SHOW TOPICS;\","
            + "\"topics\":["
            + "{\"name\":\"thetopic\",\"replicaInfo\":[1,2,3],\"consumerCount\":42,\"consumerGroupCount\":12}"
            + "],\"warnings\":[]}",
        json);

    final KafkaTopicsList actual = mapper.readValue(json, KafkaTopicsList.class);
    assertEquals(expected, actual);
  }
}
