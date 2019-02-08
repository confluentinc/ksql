/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.serde.DataSource.DataSourceSerDe;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClusterTerminatorTest {

  private static final String SOURCE_SUFFIX = "_source";

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private PersistentQueryMetadata persistentQuery0;
  @Mock
  private PersistentQueryMetadata persistentQuery1;
  @Mock
  private MetaStore metaStore;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ClusterTerminator clusterTerminator;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);
    clusterTerminator = new ClusterTerminator(ksqlConfig, ksqlEngine, serviceContext);
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
    when(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("command_topic");
    when(ksqlEngine.getPersistentQueries())
        .thenReturn(ImmutableList.of(persistentQuery0, persistentQuery1));
  }

  @Test
  public void shouldClosePersistentQueries() {
    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    verify(persistentQuery0).close();
    verify(persistentQuery1).close();
  }

  @Test
  public void shouldCloseTheEngineAfterTerminatingPersistentQueries() {
    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    final InOrder inOrder = Mockito.inOrder(persistentQuery0, ksqlEngine);
    inOrder.verify(persistentQuery0).close();
    inOrder.verify(ksqlEngine).close();
  }

  @Test
  public void shouldClosePersistentQueriesBeforeDeletingTopics() {
    // Given:
    givenTopicsExistInKafka("topic1");
    givenSinkTopicsExistInMetastore("topic1");

    // When:
    clusterTerminator.terminateCluster(Collections.singletonList("topic1"));

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient, persistentQuery0);
    inOrder.verify(persistentQuery0).close();
    inOrder.verify(kafkaTopicClient).deleteTopics(Collections.singletonList("topic1"));
  }

  @Test
  public void shouldDeleteTopicListWithExplicitTopicName() throws Exception {
    //Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicsExistInMetastore("K_Foo");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("K_Foo"));
    verifySchemaDeletedForTopics("K_Foo");
  }

  @Test
  public void shouldOnlyDeleteExistingTopics() throws Exception {
    //Given:
    givenTopicsExistInKafka("K_Bar");
    givenSinkTopicsExistInMetastore("K_Foo", "K_Bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo", "K_Bar"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(ImmutableList.of("K_Bar"));
    verifySchemaDeletedForTopics("K_Foo", "K_Bar");
  }

  @Test
  public void shouldNotDeleteNonSinkTopic() throws Exception {
    // Given:
    givenNonSinkTopicsExistInMetastore("bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    verify(kafkaTopicClient, never()).deleteTopics(Collections.singletonList("bar"));
    verifySchemaNotDeletedForTopic("bar");
  }

  @Test
  public void shouldNotDeleteNonMatchingCaseSensitiveTopics() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_FOO");
    givenSinkTopicsExistInMetastore("K_FOO");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient, times(2)).deleteTopics(Collections.emptyList());
    verifySchemaNotDeletedForTopic("K_FOO");
  }


  @Test
  @SuppressWarnings("unchecked")
  public void shouldDeleteTopicListWithPattern() throws Exception {
    //Given:
    givenTopicsExistInKafka("K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    givenSinkTopicsExistInMetastore("K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    final ArgumentCaptor<Collection> argumentCaptor = ArgumentCaptor.forClass(Collection.class);

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Fo.*"));

    // Then:
    verify(kafkaTopicClient, times(2)).deleteTopics(argumentCaptor.capture());
    final Set<String> expectedArgs = ImmutableSet.of("K_Foo", "K_Fooo", "K_Fo");
    assertThat(argumentCaptor.getAllValues().get(0).size(), equalTo(expectedArgs.size()));
    assertTrue(expectedArgs.containsAll(argumentCaptor.getAllValues().get(0)));
    verifySchemaDeletedForTopics("K_Foo", "K_Fooo", "K_Fo");
    verifySchemaNotDeletedForTopic("NotMatched");
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
    givenSinkTopicsExistInMetastore("K_Foo");
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

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
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

  private void givenSinkTopicsExistInMetastore(final String... kafkaTopicNames) {
    final Map<String, KsqlTopic> ksqlTopicMap = Stream.of(kafkaTopicNames)
        .collect(Collectors.toMap(
            kafkaTopicName -> "KSQL_" + kafkaTopicName,
            kafkaTopicName -> getKsqlTopic("KSQL_" + kafkaTopicName, kafkaTopicName, true)));
    when(metaStore.getAllKsqlTopics()).thenReturn(ksqlTopicMap);
    givenTopicsUseAvroSerdes(kafkaTopicNames);
  }

  @SuppressWarnings("SameParameterValue")
  private void givenNonSinkTopicsExistInMetastore(final String kafkaTopicName) {
    when(metaStore.getAllKsqlTopics()).thenReturn(ImmutableMap.of(
        "KSQL_" + kafkaTopicName, getKsqlTopic("KSQL_" + kafkaTopicName, kafkaTopicName, false)));
    givenTopicsUseAvroSerdes(kafkaTopicName);
  }

  private void givenTopicsUseAvroSerdes(final String... topicNames) {
    for (final String topicName : topicNames) {
      final StructuredDataSource dataSource = mock(StructuredDataSource.class);
      final KsqlTopicSerDe ksqlTopicSerDe = mock(KsqlTopicSerDe.class);

      when(metaStore.getSourceForTopic(topicName)).thenReturn(Optional.of(dataSource));
      when(dataSource.getName()).thenReturn(topicName + SOURCE_SUFFIX);
      when(dataSource.getKsqlTopicSerde()).thenReturn(ksqlTopicSerDe);
      when(ksqlTopicSerDe.getSerDe()).thenReturn(DataSourceSerDe.AVRO);
    }
  }

  private void givenTopicsExistInKafka(final String... topicNames) {
    when(kafkaTopicClient.listTopicNames())
        .thenReturn(Stream.of(topicNames).collect(Collectors.toSet()));
  }

  private void verifySchemaDeletedForTopics(final String... topicNames) throws Exception {
    for (final String topicName : topicNames) {
      verify(schemaRegistryClient).deleteSubject(
          topicName + SOURCE_SUFFIX + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
    }
  }

  private void verifySchemaNotDeletedForTopic(final String topicName) throws Exception {
    verify(schemaRegistryClient, never()).deleteSubject(
        topicName + SOURCE_SUFFIX + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }
}
