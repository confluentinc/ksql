/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClusterTerminatorTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private PersistentQueryMetadata persistentQueryMetadata;
  @Mock
  private QueryId queryId;
  @Mock
  private MetaStore metaStore;
  @Mock
  private ServiceContext serviceContext;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ClusterTerminator clusterTerminator;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
    clusterTerminator = new ClusterTerminator(ksqlConfig, ksqlEngine, serviceContext);
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("command_topic");

  }

  @Test
  public void shouldTerminatePersistetQueries() throws Exception {
    // Given:
    givenPersistentQueries(queryId);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(ksqlEngine).terminateQuery(queryId, true);
  }

  @Test
  public void shouldCloseTheEngineAfterTerminatingPersistetQueries() throws Exception {
    // Given:
    givenPersistentQueries(queryId);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(ksqlEngine).close();
  }

  @Test
  public void shouldTerminateQueriesBeforeDeleteingTopics() {
    // Given:
    givenTopicsExistInKafka("topic1");
    givenSinkTopicsExistInMetastore("topic1");

    // When:
    clusterTerminator.terminateCluster(Collections.singletonList("topic1"));

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient, ksqlEngine);
    inOrder.verify(ksqlEngine).getPersistentQueries();
    inOrder.verify(kafkaTopicClient).listTopicNames();
    inOrder.verify(kafkaTopicClient)
        .deleteTopics(Collections.singletonList("topic1"));
  }

  @Test
  public void shouldDeleteTopicListWithExplicitTopicName() {
    //Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicExists("K_Foo");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("K_Foo"));
  }

  @Test
  public void shouldOnlyDeleteExistingTopics() {
    //Given:
    givenTopicsExistInKafka("K_Bar");
    givenSinkTopicExists("K_Foo", "K_Bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo", "K_Bar"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(ImmutableList.of("K_Bar"));
  }

  @Test
  public void shouldNotDeleteNonSinkTopic() {
    // Given:
    givenNonSinkTopicExits("bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    verify(kafkaTopicClient, never()).deleteTopics(Collections.singletonList("bar"));
  }

  @Test
  public void shouldNotDeleteNonMatchingCaseSensitiveTopics() {
    // Given:
    givenTopicsExistInKafka("K_FOO");
    givenSinkTopicExists("K_FOO");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient, times(2)).deleteTopics(Collections.emptyList());
  }


  @Test
  public void shouldDeleteTopicListWithPattern() {
    //Given:
    givenTopicsExistInKafka("K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    givenSinkTopicsExistInMetastore("K_Fo", "K_Foo", "K_Fooo", "NotMatched");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Fo.*"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(ImmutableList.of("K_Fooo", "K_Fo", "K_Foo"));
  }

  @Test
  public void shouldRemoveNonExistentTopicsOnEachDeleteAttempt() {
    //Given:
    givenSinkTopicsExistInMetastore("Foo", "Bar");

    when(kafkaTopicClient.listTopicNames())
        .thenReturn(ImmutableSet.of("Other", "Foo", "Bar"))
        .thenReturn(ImmutableSet.of("Other", "Bar"))
        .thenReturn(ImmutableSet.of("Other"));

    doThrow(KsqlException.class).when(kafkaTopicClient).deleteTopics(any());
    doNothing().when(kafkaTopicClient).deleteTopics(Collections.emptyList());

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("Foo", "Bar"));

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient);
    inOrder.verify(kafkaTopicClient).deleteTopics(ImmutableList.of("Bar", "Foo"));
    inOrder.verify(kafkaTopicClient).deleteTopics(ImmutableList.of("Bar"));
    inOrder.verify(kafkaTopicClient).deleteTopics(ImmutableList.of());
  }

  @Test
  public void shouldThrowIfCouldNotDeleteTopicListWithPattern() {
    // Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicExists("K_Foo");
    doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .when(kafkaTopicClient).deleteTopics(Collections.singletonList("K_Foo"));
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Exception while deleting topics: K_Foo");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Fo*"));

  }

  @Test
  public void shouldDeleteCommandTopic() {
    // Given:
    givenTopicsExistInKafka("_confluent-ksql-command_topic_command_topic", "topic1", "topic2");

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient, ksqlEngine);
    inOrder.verify(kafkaTopicClient).listTopicNames();
    inOrder.verify(kafkaTopicClient)
        .deleteTopics(Collections.singletonList("_confluent-ksql-command_topic_command_topic"));
  }

  @Test
  public void shouldThrowIfCannotDeleteCommandTopic() {
    // Given:
    givenTopicsExistInKafka("_confluent-ksql-command_topic_command_topic");
    doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .when(kafkaTopicClient)
        .deleteTopics(Collections.singletonList("_confluent-ksql-command_topic_command_topic"));
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Exception while deleting topics: _confluent-ksql-command_topic_command_topic");

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

  }

  private static KsqlTopic getKsqlTopic(final String topicName, final String kafkaTopicName,
      final boolean isSink) {
    return new KsqlTopic(topicName, kafkaTopicName, null, isSink);
  }

  private void givenPersistentQueries(final QueryId queryId) {
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(persistentQueryMetadata));
    when(persistentQueryMetadata.getQueryId()).thenReturn(queryId);
  }

  private void givenSinkTopicExists(final String... kafkaTopicNames) {
    final Map<String, KsqlTopic> ksqlTopicMap = Stream.of(kafkaTopicNames)
        .collect(Collectors.toMap(
            kafkaTopicName -> "KSQL_" + kafkaTopicName,
            kafkaTopicName -> getKsqlTopic("KSQL_" + kafkaTopicName, kafkaTopicName, true)));
    when(metaStore.getAllKsqlTopics()).thenReturn(ksqlTopicMap);
  }

  private void givenNonSinkTopicExits(final String kafkaTopicName) {
    when(metaStore.getAllKsqlTopics()).thenReturn(ImmutableMap.of(
        "KSQL_" + kafkaTopicName, getKsqlTopic("KSQL_" + kafkaTopicName, kafkaTopicName, false)));
  }

  private void givenSinkTopicsExistInMetastore(final String... topicNames) {
    when(metaStore.getAllKsqlTopics()).thenReturn(Stream.of(topicNames)
        .map(topicName -> getKsqlTopic(topicName, topicName, true))
        .collect(Collectors.toMap(KsqlTopic::getTopicName, topic -> topic)));
  }

  private void givenTopicsExistInKafka(final String... topicNames) {
    when(kafkaTopicClient.listTopicNames())
        .thenReturn(Stream.of(topicNames).collect(Collectors.toSet()));
  }
}
