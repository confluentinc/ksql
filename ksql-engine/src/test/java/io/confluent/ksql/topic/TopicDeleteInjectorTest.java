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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
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
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicDeleteInjectorTest {

  private static final String SOURCE_NAME = "SOMETHING";
  private static final String TOPIC_NAME = "something";
  private static final ConfiguredStatement<DropStream> DROP_WITH_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING DELETE TOPIC",
      new DropStream(QualifiedName.of(SOURCE_NAME), false, true));
  private static final ConfiguredStatement<DropStream> DROP_WITHOUT_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING",
      new DropStream(QualifiedName.of(SOURCE_NAME), false, false));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private DataSource<?> source;
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
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(Format.JSON)));
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
    assertThat(injected.getStatementText(), is("DROP STREAM SOMETHING;"));
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
  public void shouldDeleteSchemaInSR() throws IOException, RestClientException {
    // Given:
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(Format.AVRO)));

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient).deleteSubject("something" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  @Test
  public void shouldNotDeleteSchemaInSRIfNotAvro() throws IOException, RestClientException {
    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldThrowExceptionIfSourceDoesNotExist() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING", new DropStream(QualifiedName.of("SOMETHING_ELSE"), true, true));

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Could not find source to delete topic for");

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldNotThrowIfNoOtherSourcesUsingTopic() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING DELETE TOPIC;",
        new DropStream(QualifiedName.of(SOURCE_NAME),
            true,
            true)
    );
    final DataSource<?> other1 = givenSource("OTHER", "other");
    final Map<String, DataSource<?>> sources = new HashMap<>();
    sources.put(SOURCE_NAME, source);
    sources.put("OTHER", other1);
    when(metaStore.getAllDataSources()).thenReturn(sources);

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldThrowExceptionIfOtherSourcesUsingTopic() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING DELETE TOPIC;",
        new DropStream(QualifiedName.of(SOURCE_NAME),
            true,
            true)
    );
    final DataSource<?> other1 = givenSource("OTHER1", TOPIC_NAME);
    final DataSource<?> other2 = givenSource("OTHER2", TOPIC_NAME);
    final Map<String, DataSource<?>> sources = new HashMap<>();
    sources.put(SOURCE_NAME, source);
    sources.put("OTHER1", other1);
    sources.put("OTHER2", other2);
    when(metaStore.getAllDataSources()).thenReturn(sources);

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Refusing to delete topic. "
            + "Found other data sources (OTHER1, OTHER2) using topic something");

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldNotThrowIfSchemaIsMissing() throws IOException, RestClientException {
    // Given:
    when(topic.getValueFormat())
        .thenReturn(ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("foo"))));

    doThrow(new RestClientException("Subject not found.", 404, 40401))
            .when(registryClient).deleteSubject("something" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);
  }

  private static DataSource<?> givenSource(final String name, final String topicName) {
    final DataSource source = mock(DataSource.class);
    when(source.getName()).thenReturn(name);
    when(source.getKafkaTopicName()).thenReturn(topicName);
    return source;
  }

  private static <T extends Statement> ConfiguredStatement<T> givenStatement(
      final String text,
      final T statement
  ) {
    return ConfiguredStatement.of(
        PreparedStatement.of(text, statement),
        ImmutableMap.of(),
        new KsqlConfig(ImmutableMap.of())
    );
  }
}