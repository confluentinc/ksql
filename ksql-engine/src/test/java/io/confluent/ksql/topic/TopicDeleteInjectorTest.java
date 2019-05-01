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
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
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

  private static final ConfiguredStatement<DropStream> DROP_WITH_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING DELETE TOPIC",
      new DropStream(QualifiedName.of("SOMETHING"), false, true));
  private static final ConfiguredStatement<DropStream> DROP_WITHOUT_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING",
      new DropStream(QualifiedName.of("SOMETHING"), false, false));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private DataSource<?> source;
  @Mock
  private SchemaRegistryClient registryClient;
  @Mock
  private KafkaTopicClient topicClient;

  private TopicDeleteInjector deleteInjector;

  @Before
  public void setUp() {
    deleteInjector = new TopicDeleteInjector(metaStore, topicClient, registryClient);

    when(metaStore.getSource("SOMETHING")).thenAnswer(inv -> source);
    when(source.getKafkaTopicName()).thenReturn("something");
    when(source.getKsqlTopicSerde()).thenReturn(new KsqlJsonTopicSerDe());
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
    when(source.getKsqlTopicSerde()).thenReturn(new KsqlAvroTopicSerDe("foo"));

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
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not find source to delete topic for");

    // When:
    deleteInjector.inject(dropStatement);
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