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
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KafkaTopicException;

import static org.easymock.EasyMock.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaTopicClientTest {

  private Node node;

  private final String topicName1 = "topic1";
  private final String topicName2 = "ksql_query_2-KSTREAM-MAP-0000000012-repartition";
  private final String topicName3 = "ksql_query_2-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog";

  @Before
  public void init() {
    node = new Node(1,"host", 9092);
  }

  @Test
  public void testCreateTopic() {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.createTopics(anyObject())).andReturn(getCreateTopicsResult());
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());

    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic("test", 1, (short)1);
    verify(adminClient);
  }

  @Test
  public void shouldUseExistingTopicWIthTheSameSpecsInsteadOfCreate() {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short)1);
    verify(adminClient);
  }


  @Test(expected = KafkaTopicException.class)
  public void shouldFailCreateExistingTopic() {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.createTopics(anyObject())).andReturn(getCreateTopicsResult());
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short)2);
    verify(adminClient);
  }

  @Test
  public void shouldNotFailIfTopicAlreadyExistsWhenCreating() throws InterruptedException,
                                                                      ExecutionException {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.createTopics(anyObject())).andReturn(createTopicReturningTopicExistsException());
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short)1);
    verify(adminClient);
  }

  @Test
  public void shouldRetryDescribeTopicOnRetriableException() throws InterruptedException,
                                                                    ExecutionException {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.createTopics(anyObject())).andReturn(createTopicReturningTopicExistsException());
    expect(adminClient.describeTopics(anyObject()))
        .andReturn(describeTopicReturningUnknownPartitionException()).once();
    // The second time, return the right response.
    expect(adminClient.describeTopics(anyObject())).andReturn(getDescribeTopicsResult()).once();
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.createTopic(topicName1, 1, (short)1);
    verify(adminClient);
  }

  @Test(expected = KafkaResponseGetFailedException.class)
  public void shouldFailToDescribeTopicsWhenRetriesExpire() throws InterruptedException,
                                                                 ExecutionException {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(getEmptyListTopicResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.describeTopics(anyObject()))
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException())
        .andReturn(describeTopicReturningUnknownPartitionException());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.describeTopics(Collections.singleton(topicName1));
    verify(adminClient);
  }

  @Test
  public void shouldRetryListTopics() throws InterruptedException, ExecutionException {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(listTopicResultWithNotControllerException()).once();
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    Set<String> names = kafkaTopicClient.listTopicNames();
    assertThat(names, equalTo(Utils.mkSet(topicName1, topicName2, topicName3)));
    verify(adminClient);
  }

  @Test
  public void testListTopicNames() {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    Set<String> names = kafkaTopicClient.listTopicNames();
    assertThat(names, equalTo(Utils.mkSet(topicName1, topicName2, topicName3)));
    verify(adminClient);
  }

  @Test
  public void testDeleteTopics() {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.deleteTopics(anyObject())).andReturn(getDeleteTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    List<String> topics = Collections.singletonList(topicName2);
    kafkaTopicClient.deleteTopics(topics);
    verify(adminClient);
  }

  @Test
  public void testDeleteInternalTopics() {
    AdminClient adminClient = mock(AdminClient.class);
    expect(adminClient.describeCluster()).andReturn(getDescribeClusterResult());
    expect(adminClient.listTopics()).andReturn(getListTopicsResult());
    expect(adminClient.describeConfigs(anyObject())).andReturn(getDescribeConfigsResult());
    expect(adminClient.deleteTopics(anyObject())).andReturn(getDeleteTopicsResult());
    replay(adminClient);
    KafkaTopicClient kafkaTopicClient = new KafkaTopicClientImpl(adminClient);
    kafkaTopicClient.deleteInternalTopics("ksql_query_2");
    verify(adminClient);
  }

  /**
   *
   * Utility functions
   */

  private DescribeTopicsResult describeTopicReturningUnknownPartitionException()
      throws InterruptedException, ExecutionException {
    DescribeTopicsResult describeTopicsResult = niceMock(DescribeTopicsResult.class);
    KafkaFuture resultFuture = niceMock(KafkaFuture.class);
    expect(describeTopicsResult.all()).andReturn(resultFuture);
    expect(resultFuture.get()).andThrow(new ExecutionException(
        new UnknownTopicOrPartitionException("Topic doesn't exist")
    ));

    replay(describeTopicsResult, resultFuture);

    return describeTopicsResult;
  }

  private DescribeTopicsResult getDescribeTopicsResult() {
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
    replay(describeTopicsResult);
    return describeTopicsResult;
  }

  private CreateTopicsResult createTopicReturningTopicExistsException() throws InterruptedException,
                                                                               ExecutionException{
    CreateTopicsResult createTopicsResult = niceMock(CreateTopicsResult.class);
    KafkaFuture resultFuture = niceMock(KafkaFuture.class);
    expect(createTopicsResult.all()).andReturn(resultFuture);

    expect(resultFuture.get()).andThrow(new ExecutionException(
        new TopicExistsException("Topic already exists")));

    replay(createTopicsResult, resultFuture);
    return createTopicsResult;
  }

  private CreateTopicsResult getCreateTopicsResult() {
    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    expect(createTopicsResult.all()).andReturn(KafkaFuture.allOf());
    replay(createTopicsResult);
    return createTopicsResult;
  }

  private DeleteTopicsResult getDeleteTopicsResult() {
    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    expect(deleteTopicsResult.values()).andReturn(Collections.singletonMap(topicName1, KafkaFuture
        .allOf()));
    replay(deleteTopicsResult);
    return deleteTopicsResult;
  }

  private ListTopicsResult getEmptyListTopicResult() {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    List<String> topicNamesList = Arrays.asList();
    expect(listTopicsResult.names()).andReturn(KafkaFuture.completedFuture(new HashSet<>
                                                                               (topicNamesList)));
    replay(listTopicsResult);
    return listTopicsResult;
  }


  private ListTopicsResult listTopicResultWithNotControllerException() throws InterruptedException,
                                                                              ExecutionException {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    KafkaFuture resultFuture = niceMock(KafkaFuture.class);
    expect(listTopicsResult.names()).andReturn(resultFuture);
    expect(resultFuture.get()).andThrow(new ExecutionException(
        new NotControllerException("Not Controller")));
    replay(listTopicsResult, resultFuture);
    return listTopicsResult;
  }

  private ListTopicsResult getListTopicsResult() {
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    List<String> topicNamesList = Arrays.asList(topicName1, topicName2, topicName3);
    expect(listTopicsResult.names()).andReturn(KafkaFuture.completedFuture(new HashSet<>
                                                                               (topicNamesList)));
    replay(listTopicsResult);
    return listTopicsResult;
  }

  private DescribeClusterResult getDescribeClusterResult() {
    Collection<Node> nodes = Collections.singletonList(node);
    DescribeClusterResult describeClusterResult =  mock(DescribeClusterResult.class);
    expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(nodes));
    replay(describeClusterResult);
    return describeClusterResult;
  }

  private DescribeConfigsResult getDescribeConfigsResult() {
    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    ConfigEntry configEntry = new ConfigEntry("delete.topic.enable", "true");
    Map<ConfigResource, Config> config = new HashMap<>();
    config.put(new ConfigResource(ConfigResource.Type.BROKER, "1"), new Config
        (Collections.singletonList(configEntry)));
    expect(describeConfigsResult.all()).andReturn(KafkaFuture.completedFuture(config));
    replay(describeConfigsResult);
    return describeConfigsResult;
  }
}
