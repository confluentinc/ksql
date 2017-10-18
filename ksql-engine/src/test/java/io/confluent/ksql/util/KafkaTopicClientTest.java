/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.confluent.ksql.exception.KafkaTopicException;

import static org.easymock.EasyMock.*;

public class KafkaTopicClientTest {

  private AdminClient adminClient;

  private KafkaTopicClient kafkaTopicClient;

  String topicName1 = "topic1";
  String topicName2 = "ksql_query_2-KSTREAM-MAP-0000000012-repartition";
  String topicName3 = "ksql_query_2-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog";

  @Before
  public void init() throws ExecutionException, InterruptedException, IllegalAccessException,
                            InstantiationException {

    // Mock values.
    Node node = new Node(1,"host", 9092);
    Collection<Node> nodes = Collections.singletonList(node);
    DescribeClusterResult describeClusterResult =  mock(DescribeClusterResult.class);
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(nodes));

    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    ConfigEntry configEntry = new ConfigEntry("delete.topic.enable", "true");
    Map<ConfigResource, Config> config = new HashMap<>();
    config.put(new ConfigResource(ConfigResource.Type.BROKER, "1"), new Config
        (Collections.singletonList(configEntry)));
    expect(describeConfigsResult.all()).andReturn(KafkaFuture.completedFuture(config));
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    List<String> topicNamesList = Arrays.asList(topicName1, topicName2, topicName3);
    expect(listTopicsResult.names()).andReturn(KafkaFuture.completedFuture(new HashSet<>
                                                                               (topicNamesList)));

    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    expect(deleteTopicsResult.values()).andReturn(Collections.singletonMap(topicName1, KafkaFuture
        .allOf()));


    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    KafkaFutureImpl<Optional<Void>> voidKafkaFuture = new KafkaFutureImpl<Optional<Void>>();
    expect(createTopicsResult.all()).andReturn(KafkaFuture.allOf());

    TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(0, node, Collections
        .singletonList(node), Collections.singletonList(node));
    TopicDescription topicDescription = new TopicDescription(topicName1, false,
                                                             Collections.singletonList
                                                                 (topicPartitionInfo));
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    expect(describeTopicsResult.all()).andReturn(KafkaFuture.completedFuture(Collections
                                                                                 .singletonMap
                                                                                     (topicName1,
                                                                                      topicDescription
                                                                                      )));

    adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(describeClusterResult);
    expect(adminClient.createTopics(anyObject())).andReturn(createTopicsResult);
    expect(adminClient.deleteTopics(anyObject())).andReturn(deleteTopicsResult);
    expect(adminClient.describeConfigs(anyObject())).andReturn(describeConfigsResult);
    expect(adminClient.listTopics()).andReturn(listTopicsResult);
    expect(adminClient.describeTopics(anyObject())).andReturn(describeTopicsResult);

    replay(describeClusterResult);
    replay(describeConfigsResult);
    replay(describeTopicsResult);
    replay(deleteTopicsResult);
    replay(createTopicsResult);
    replay(listTopicsResult);
    replay(adminClient);
    kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
  }

  @Test
  public void testCreateTopic() {
    kafkaTopicClient.createTopic("test", 1, (short)1);
  }

  @Test
  public void testCreateExistingTopic() {
    kafkaTopicClient.createTopic(topicName1, 1, (short)1);
  }

  @Test
  public void shouldFailCreateExistingTopic() {
    boolean failed = false;
    try {
      kafkaTopicClient.createTopic(topicName1, 1, (short)2);
    } catch (KafkaTopicException e) {
      failed = true;
    }
    Assert.assertTrue(failed);
  }

  @Test
  public void testListTopicNames() {
    Set<String> names = kafkaTopicClient.listTopicNames();
    Assert.assertTrue(names.size() == 3);
    Assert.assertTrue(names.contains(topicName1));
    Assert.assertTrue(names.contains(topicName2));
    Assert.assertTrue(names.contains(topicName3));
  }

  @Test
  public void testDeleteTopics() {
    List<String> topics = Collections.singletonList(topicName2);
    kafkaTopicClient.deleteTopics(topics);
  }

  @Test
  public void testDeleteInteralTopics() {
    List<String> topics = Collections.singletonList(topicName2);
    kafkaTopicClient.deleteInternalTopics("ksql_query_2");
  }

}
