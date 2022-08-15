/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.topic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicDeleteInjectorTest {

  private static final SourceName SOURCE_NAME = SourceName.of("SOMETHING");
  private static final String TOPIC_NAME = "something";
  private static final ConfiguredStatement<DropStream> DROP_WITH_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING DELETE TOPIC",
      new DropStream(SOURCE_NAME, false, true));
  private static final ConfiguredStatement<DropStream> DROP_WITHOUT_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING",
      new DropStream(SOURCE_NAME, false, false));

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private DataSource source;
  @Mock
  private KsqlTopic topic;
  @Mock
  private SchemaRegistryClient registryClient;
  @Mock
  private KafkaTopicClient topicClient;

  private TopicDeleteInjector deleteInjector;

  @Before
  public void setUp() {
    deleteInjector = new TopicDeleteInjector(metaStore, topicClient, registryClient);

    when(metaStore.getSource(SOURCE_NAME)).thenAnswer(inv -> source);
    when(source.getName()).thenReturn(SOURCE_NAME);
    when(source.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(source.getKsqlTopic()).thenReturn(topic);
    when(topic.getKeyFormat()).thenReturn(KeyFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of(), Optional.empty()));
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of()));
  }

  @Test
  public void shouldDoNothingForNonDropStatements() {
    // Given:
    final ConfiguredStatement<ListProperties> listProperties =
        givenStatement("LIST", new ListProperties(Optional.empty()));

    // When:
    final ConfiguredStatement<ListProperties> injected = deleteInjector.inject(listProperties);

    // Then:
    assertThat(injected, is(sameInstance(listProperties)));
  }

  @Test
  public void shouldDoNothingIfNoDeleteTopic() {
    // When:
    final ConfiguredStatement<DropStream> injected = deleteInjector.inject(DROP_WITHOUT_DELETE_TOPIC);

    // Then:
    assertThat(injected, is(sameInstance(DROP_WITHOUT_DELETE_TOPIC)));
    verifyNoMoreInteractions(topicClient, registryClient);
  }

  @Test
  public void shouldDropTheDeleteTopicClause() {
    // When:
    final ConfiguredStatement<DropStream> injected = deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    assertThat(injected.getMaskedStatementText(), is("DROP STREAM SOMETHING;"));
    assertThat("expected !isDeleteTopic", !injected.getStatement().isDeleteTopic());
  }

  @Test
  public void shouldDeleteTopic() {
    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(topicClient).deleteTopics(ImmutableList.of("something"));
  }

  @Test
  public void shouldDeleteAvroSchemaInSR() throws IOException, RestClientException {
    // Given:
    when(topic.getKeyFormat()).thenReturn(KeyFormat.of(FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of(), Optional.empty()));
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()),
        SerdeFeatures.of()));

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient).deleteSubject(KsqlConstants.getSRSubject("something", true));
    verify(registryClient).deleteSubject(KsqlConstants.getSRSubject("something", false));
  }

  @Test
  public void shouldDeleteValueAvroSchemaInSrEvenIfKeyDeleteFails() throws IOException, RestClientException {
    // Given:
    when(topic.getKeyFormat()).thenReturn(KeyFormat.of(FormatInfo.of(FormatFactory.AVRO.name()), SerdeFeatures.of(), Optional.empty()));
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()),
        SerdeFeatures.of()));
    doThrow(new KsqlException("foo"))
        .when(registryClient)
        .deleteSubject(KsqlConstants.getSRSubject("something", true));

    // When:
    assertThrows(KsqlException.class, () -> deleteInjector.inject(DROP_WITH_DELETE_TOPIC));

    // Then:
    verify(registryClient).deleteSubject(KsqlConstants.getSRSubject("something", false));
  }

  @Test
  public void shouldDeleteProtoSchemaInSR() throws IOException, RestClientException {
    // Given:
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.PROTOBUF.name()),
        SerdeFeatures.of()));

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient).deleteSubject(KsqlConstants.getSRSubject("something", false));
  }

  @Test
  public void shouldNotDeleteSchemaInSRIfNotSRSupported() throws IOException, RestClientException {
    // Given:
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(FormatFactory.DELIMITED.name()),
        SerdeFeatures.of()));

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldThrowExceptionIfSourceDoesNotExist() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING", new DropStream(SourceName.of("SOMETHING_ELSE"), false, true));

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> deleteInjector.inject(dropStatement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not find source to delete topic for"));
  }

  @Test
  public void shouldNotThrowIfStatementHasIfExistsAndSourceDoesNotExist() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING", new DropStream(SourceName.of("SOMETHING_ELSE"), true, true));

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldNotThrowIfNoOtherSourcesUsingTopic() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING DELETE TOPIC;",
        new DropStream(SOURCE_NAME,
            true,
            true)
    );
    final DataSource other1 = givenSource(SourceName.of("OTHER"), "other");
    final Map<SourceName, DataSource> sources = new HashMap<>();
    sources.put(SOURCE_NAME, source);
    sources.put(SourceName.of("OTHER"), other1);
    when(metaStore.getAllDataSources()).thenReturn(sources);

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldThrowExceptionIfOtherSourcesUsingTopic() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING DELETE TOPIC;",
        new DropStream(SOURCE_NAME,
            true,
            true)
    );
    final DataSource other1 = givenSource(SourceName.of("OTHER1"), TOPIC_NAME);
    final DataSource other2 = givenSource(SourceName.of("OTHER2"), TOPIC_NAME);
    final Map<SourceName, DataSource> sources = new HashMap<>();
    sources.put(SOURCE_NAME, source);
    sources.put(SourceName.of("OTHER1"), other1);
    sources.put(SourceName.of("OTHER2"), other2);
    when(metaStore.getAllDataSources()).thenReturn(sources);

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> deleteInjector.inject(dropStatement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Refusing to delete topic. "
        + "Found other data sources (OTHER1, OTHER2) using topic something"));
  }

  @Test
  public void shouldThrowIfTopicDoesNotExist() {
    // Given:
    final SourceName STREAM_1 = SourceName.of("stream1");
    final DataSource other1 = givenSource(STREAM_1, "topicName");
    when(metaStore.getSource(STREAM_1)).thenAnswer(inv -> other1);
    when(other1.getKafkaTopicName()).thenReturn("topicName");
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP stream1 DELETE TOPIC;",
        new DropStream(SourceName.of("stream1"), true, true)
    );
    doThrow(RuntimeException.class).when(topicClient).deleteTopics(ImmutableList.of("topicName"));

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> deleteInjector.inject(dropStatement)
    );

    // Then:
    assertThat(e.getMessage(), containsString("" +
        "Could not delete the corresponding kafka topic: topicName"));
  }

  @Test
  public void shouldNotThrowIfSchemaIsMissing() throws IOException, RestClientException {
    // Given:
    when(topic.getKeyFormat())
        .thenReturn(KeyFormat.of(FormatInfo.of(
            FormatFactory.AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "foo")),
            SerdeFeatures.of(),
            Optional.empty()));
    when(topic.getValueFormat())
        .thenReturn(ValueFormat.of(FormatInfo.of(
            FormatFactory.AVRO.name(), ImmutableMap.of(ConnectProperties.FULL_SCHEMA_NAME, "foo")),
            SerdeFeatures.of()));

    doThrow(new RestClientException("Subject not found.", 404, 40401))
        .when(registryClient).deleteSubject(KsqlConstants.getSRSubject("something", true));
    doThrow(new RestClientException("Subject not found.", 404, 40401))
        .when(registryClient).deleteSubject(KsqlConstants.getSRSubject("something", false));

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);
  }

  @Test
  public void shouldThrowOnDeleteTopicIfSourceIsReadOnly() {
    // Given:
    when(source.isSource()).thenReturn(true);

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> deleteInjector.inject(DROP_WITH_DELETE_TOPIC)
    );

    // Then:
    assertThat(e.getMessage(), containsString("" +
        "Cannot delete topic for read-only source: SOMETHING"));
  }

  private static DataSource givenSource(final SourceName name, final String topicName) {
    final DataSource source = mock(DataSource.class);
    when(source.getName()).thenReturn(name);
    when(source.getKafkaTopicName()).thenReturn(topicName);
    return source;
  }

  private static <T extends Statement> ConfiguredStatement<T> givenStatement(
      final String text,
      final T statement
  ) {
    return ConfiguredStatement.of(PreparedStatement.of(text, statement), SessionConfig
        .of(new KsqlConfig(ImmutableMap.of()), ImmutableMap.of()));
  }
}