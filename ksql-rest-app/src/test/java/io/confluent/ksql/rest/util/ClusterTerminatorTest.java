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

package io.confluent.ksql.rest.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.TopicDeletionDisabledException;
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

  private final static String MANAGED_TOPIC_1 = "MANAGED_TOPIC_1";
  private final static String MANAGED_TOPIC_2 = "MANAGED_TOPIC_2";
  private final static List<String> MANAGED_TOPICS = ImmutableList.of(
      MANAGED_TOPIC_1,
      MANAGED_TOPIC_2
  );

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

  private final Map<String, DataSource<?>> dataSources = new HashMap<>();

  private ClusterTerminator clusterTerminator;

  @Before
  public void setup() {
    dataSources.clear();

    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);
    clusterTerminator = new ClusterTerminator(
        ksqlConfig,
        ksqlEngine,
        serviceContext,
        MANAGED_TOPICS);
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
    when(ksqlEngine.getPersistentQueries())
        .thenReturn(ImmutableList.of(persistentQuery0, persistentQuery1));

    when(metaStore.getAllDataSources()).thenReturn(dataSources);
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
    givenSinkTopicsExistInMetastore(Format.DELIMITED,"topic1");

    // When:
    clusterTerminator.terminateCluster(Collections.singletonList("topic1"));

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient, persistentQuery0);
    inOrder.verify(persistentQuery0).close();
    inOrder.verify(kafkaTopicClient).deleteTopics(Collections.singletonList("topic1"));
  }

  @Test
  public void shouldDeleteTopicListWithExplicitTopicName() {
    // Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicsExistInMetastore(Format.DELIMITED,"K_Foo");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList("K_Foo"));
  }

  @Test
  public void shouldCleanUpSchemasForExplicitTopicList() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicsExistInMetastore(Format.AVRO, "K_Foo");
    givenSchemasForTopicsExistInSchemaRegistry("K_Foo");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verifySchemaDeletedForTopics("K_Foo");
  }

  @Test
  public void shouldOnlyDeleteExistingTopics() {
    // Given:
    givenTopicsExistInKafka("K_Bar");
    givenSinkTopicsExistInMetastore(Format.JSON, "K_Foo", "K_Bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo", "K_Bar"));

    // Then:
    verify(kafkaTopicClient).deleteTopics(ImmutableList.of("K_Bar"));
  }

  @Test
  public void shouldCleanUpSchemaEvenIfTopicDoesNotExist() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Bar");
    givenSinkTopicsExistInMetastore(Format.AVRO, "K_Foo", "K_Bar");
    givenSchemasForTopicsExistInSchemaRegistry("K_Foo", "K_Bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo", "K_Bar"));

    // Then:
    verifySchemaDeletedForTopics("K_Foo", "K_Bar");
  }

  @Test
  public void shouldNotCleanUpSchemaIfSchemaDoesNotExist() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Foo", "K_Bar");
    givenSinkTopicsExistInMetastore(Format.AVRO, "K_Foo", "K_Bar");
    givenSchemasForTopicsExistInSchemaRegistry("K_Bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo", "K_Bar"));

    // Then:
    verifySchemaDeletedForTopics("K_Bar");
    verifySchemaNotDeletedForTopic("K_Foo");
  }

  @Test
  public void shouldNotDeleteNonSinkTopic() {
    // Given:
    givenTopicsExistInKafka("bar");
    givenNonSinkTopicsExistInMetastore("bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    verify(kafkaTopicClient, never()).deleteTopics(Collections.singletonList("bar"));
  }

  @Test
  public void shouldNotCleanUpSchemaForNonSinkTopic() throws Exception {
    // Given:
    givenTopicsExistInKafka("bar");
    givenNonSinkTopicsExistInMetastore("bar");
    givenSchemasForTopicsExistInSchemaRegistry("bar");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("bar"));

    // Then:
    verifySchemaNotDeletedForTopic("bar");
  }

  @Test
  public void shouldNotDeleteNonMatchingCaseSensitiveTopics() {
    // Given:
    givenTopicsExistInKafka("K_FOO");
    givenSinkTopicsExistInMetastore(Format.AVRO, "K_FOO");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verify(kafkaTopicClient, times(2)).deleteTopics(Collections.emptyList());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldDeleteTopicListWithPattern() {
    // Given:
    givenTopicsExistInKafka("K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    givenSinkTopicsExistInMetastore(Format.JSON, "K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    final ArgumentCaptor<Collection<String>> argumentCaptor = ArgumentCaptor
        .forClass(Collection.class);

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Fo.*"));

    // Then:
    verify(kafkaTopicClient, times(2)).deleteTopics(argumentCaptor.capture());
    final Set<String> expectedArgs = ImmutableSet.of("K_Foo", "K_Fooo", "K_Fo");
    assertThat(argumentCaptor.getAllValues().get(0).size(), equalTo(expectedArgs.size()));
    assertTrue(expectedArgs.containsAll(argumentCaptor.getAllValues().get(0)));
  }

  @Test
  public void shouldCleanUpSchemasForTopicListWithPattern() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    givenSinkTopicsExistInMetastore(Format.AVRO, "K_Fo", "K_Foo", "K_Fooo", "NotMatched");
    givenSchemasForTopicsExistInSchemaRegistry("K_Fo", "K_Foo", "K_Fooo", "NotMatched");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Fo.*"));

    // Then:
    verifySchemaDeletedForTopics("K_Foo", "K_Fooo", "K_Fo");
    verifySchemaNotDeletedForTopic("NotMatched");
  }

  @Test
  public void shouldRemoveNonExistentTopicsOnEachDeleteAttempt() {
    //Given:
    givenSinkTopicsExistInMetastore(Format.JSON, "Foo", "Bar");

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
    givenSinkTopicsExistInMetastore(Format.DELIMITED, "K_Foo");
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
  public void shouldDeleteManagedTopics() {
    // Given:
    givenTopicsExistInKafka(MANAGED_TOPIC_1, MANAGED_TOPIC_2);

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaTopicClient, ksqlEngine);
    inOrder.verify(kafkaTopicClient).listTopicNames();
    inOrder.verify(kafkaTopicClient).deleteTopics(MANAGED_TOPICS);
  }

  @Test
  public void shouldThrowIfCannotDeleteManagedTopic() {
    // Given:
    givenTopicsExistInKafka(MANAGED_TOPIC_1, MANAGED_TOPIC_2);
    doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .doThrow(KsqlException.class)
        .when(kafkaTopicClient)
        .deleteTopics(MANAGED_TOPICS);
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Exception while deleting topics: MANAGED_TOPIC_1, MANAGED_TOPIC_2");

    // When:
    clusterTerminator.terminateCluster(Collections.emptyList());

  }

  @Test
  public void shouldNotThrowOnTopicDeletionDisabledException() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicsExistInMetastore(Format.AVRO,"K_Foo");
    givenSchemasForTopicsExistInSchemaRegistry("K_Foo");
    doThrow(TopicDeletionDisabledException.class).when(kafkaTopicClient).deleteTopics(any());

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verifySchemaDeletedForTopics("K_Foo");
  }

  @Test
  public void shouldNotThrowIfCannotCleanUpSchema() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Foo", "K_Bar");
    givenSinkTopicsExistInMetastore(Format.AVRO,"K_Foo", "K_Bar");
    givenSchemasForTopicsExistInSchemaRegistry("K_Foo", "K_Bar");
    when(schemaRegistryClient.deleteSubject(startsWith("K_Foo")))
        .thenThrow(new RestClientException("bad", 404, 40401));

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo", "K_Bar"));

    // Then:
    verifySchemaDeletedForTopics("K_Bar");
  }

  @Test
  public void shouldNotCleanUpSchemaForNonAvroTopic() throws Exception {
    // Given:
    givenTopicsExistInKafka("K_Foo");
    givenSinkTopicsExistInMetastore(Format.JSON,"K_Foo");
    givenSchemasForTopicsExistInSchemaRegistry("K_Foo");

    // When:
    clusterTerminator.terminateCluster(ImmutableList.of("K_Foo"));

    // Then:
    verifySchemaNotDeletedForTopic("K_Foo");
  }

  private void givenSinkTopicsExistInMetastore(
      final Format format,
      final String... kafkaTopicNames
  ) {
    Stream.of(kafkaTopicNames)
        .forEach(name -> givenSourceRegisteredWithTopic(format, name, true));
  }

  @SuppressWarnings("SameParameterValue")
  private void givenNonSinkTopicsExistInMetastore(final String kafkaTopicName) {
    givenSourceRegisteredWithTopic(Format.AVRO, kafkaTopicName, false);
  }

  private void givenSourceRegisteredWithTopic(
      final Format format,
      final String kafkaTopicName,
      final boolean sink
  ) {
    final String sourceName = "SOURCE_" + kafkaTopicName;

    final KsqlSerdeFactory valueSerdeFactory = mock(KsqlSerdeFactory.class);
    when(valueSerdeFactory.getFormat()).thenReturn(format);

    final KsqlTopic topic = mock(KsqlTopic.class);
    when(topic.getKafkaTopicName()).thenReturn(kafkaTopicName);
    when(topic.isKsqlSink()).thenReturn(sink);
    when(topic.getValueSerdeFactory()).thenReturn(valueSerdeFactory);

    final DataSource<?> source = mock(DataSource.class);
    when(source.getKsqlTopic()).thenReturn(topic);

    assertThat("topic already registered", dataSources.put(sourceName, source), is(nullValue()));
  }

  private void givenTopicsExistInKafka(final String... topicNames) {
    when(kafkaTopicClient.listTopicNames())
        .thenReturn(Stream.of(topicNames).collect(Collectors.toSet()));
  }

  private void givenSchemasForTopicsExistInSchemaRegistry(final String... topicNames) throws Exception {
    final Collection<String> subjectNames = Stream.of(topicNames)
        .map(topicName -> topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX)
        .collect(Collectors.toList());
    when(schemaRegistryClient.getAllSubjects()).thenReturn(subjectNames);
  }

  private void verifySchemaDeletedForTopics(final String... topicNames) throws Exception {
    for (final String topicName : topicNames) {
      verify(schemaRegistryClient).deleteSubject(
          topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
    }
  }

  private void verifySchemaNotDeletedForTopic(final String topicName) throws Exception {
    verify(schemaRegistryClient, never()).deleteSubject(
        topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }
}
