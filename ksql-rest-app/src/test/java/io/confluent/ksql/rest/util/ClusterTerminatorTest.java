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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("command_topic");

  }

  @Test
  public void shouldTerminatePersistetQueries() throws Exception {
    // Given:
    givenPersistentQueries(queryId);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(ksqlEngine).terminateQuery(same(queryId), eq(true));
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
  public void shouldDeleteTopicListWithExplicitTopicName() {
    //Given:
    givenSinkTopicExists("FOO", "K_FOO");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_FOO"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("K_FOO"));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotDeleteTopicNonSinkTopic() {
    // Given:
    givenNonSinkTopicExits("BAR", "bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));
  }

  @Test
  public void shouldNotDeleteNonMatchingCaseSensitiveTopics() {
    // Given:
    givenSinkTopicExists("FOO", "K_FOO");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.emptyList());
  }


  @Test
  public void shouldDeleteTopicListWithPattern() {
    //Given:
    givenSinkTopicsExist("K_FO", "K_FOO", "K_FOOO", "NotMatched");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_FO.*"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(ImmutableList.of("K_FOOO", "K_FO", "K_FOO"));
  }

  @Test
  public void shouldThrowIfCouldNotDeleteTopicListWithPattern() {
    // Given:
    givenSinkTopicExists("FOO", "K_FOO");
    doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .when(kafkaTopicClient).deleteTopics(Collections.singletonList("K_FOO"));
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Exception while deleting topics: K_FOO");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_FO*"));

  }

  @Test
  public void shouldThrowIfDeleteTopicListMatchesNonSink() {
    // Given:
    givenNonSinkTopicExits("BAR", "BAR");
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid request: BAR is not a KSQL sink topic.");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("BAR"));
  }

  @Test
  public void shouldDeleteCommandTopic() {
    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient, ksqlEngine);
    inOrder.verify(ksqlEngine).getPersistentQueries();
    inOrder.verify(kafkaTopicClient)
        .deleteTopics(Collections.singletonList("_confluent-ksql-command_topic_command_topic"));
    inOrder.verify(ksqlEngine).close();
  }

  @Test
  public void shouldThrowIfCannotDeleteCommandTopic() {
    // Given:

    doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .when(kafkaTopicClient)
        .deleteTopics(Collections.singletonList("_confluent-ksql-command_topic_command_topic"));
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Could not delete the command topic: _confluent-ksql-command_topic_command_topic");

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(kafkaTopicClient)
        .deleteTopics(Collections.singletonList("_confluent-ksql-command_topic_command_topic"));
  }

  private static KsqlTopic getKsqlTopic(final String topicName, final String kafkaTopicName,
      final boolean isSink) {
    return new KsqlTopic(topicName, kafkaTopicName, null, isSink);
  }

  private void givenPersistentQueries(final QueryId queryId) {
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(persistentQueryMetadata));
    when(persistentQueryMetadata.getQueryId()).thenReturn(queryId);
  }

  private void givenSinkTopicExists(final String topicName, final String kafkaTopicName) {
    when(metaStore.getAllKsqlTopics()).thenReturn(ImmutableMap.of(
        topicName, getKsqlTopic(topicName, kafkaTopicName, true)));
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
  }

  private void givenNonSinkTopicExits(final String topicName, final String kafkaTopicName) {
    when(metaStore.getAllKsqlTopics()).thenReturn(ImmutableMap.of(
        topicName, getKsqlTopic(topicName, kafkaTopicName, false)));
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
  }

  private void givenSinkTopicsExist(final String... topicNames) {
    when(metaStore.getAllKsqlTopics()).thenReturn(Stream.of(topicNames)
        .map(topicName -> getKsqlTopic(topicName, topicName, true))
        .collect(Collectors.toMap(KsqlTopic::getTopicName, topic -> topic)));
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
  }
}
