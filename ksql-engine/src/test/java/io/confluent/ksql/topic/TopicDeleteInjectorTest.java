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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.DataSource.DataSourceSerDe;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicDeleteInjectorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private StructuredDataSource<?> source;
  @Mock
  private SchemaRegistryClient registryClient;
  @Mock
  private FakeKafkaTopicClient topicClient;

  private TopicDeleteInjector deleteInjector;
  private ConfiguredStatement<DropStream> dropStatement;

  @Before
  public void setUp() {
    deleteInjector = new TopicDeleteInjector(metaStore, topicClient, registryClient);

    when(metaStore.getSource("SOMETHING")).thenAnswer(inv -> source);
    when(source.isSerdeFormat(DataSourceSerDe.AVRO)).thenReturn(false);
    when(source.getKafkaTopicName()).thenReturn("something");

    dropStatement = givenStatement(
        "DROP SOMETHING", new DropStream(QualifiedName.of("SOMETHING"), false, true));
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
    final ConfiguredStatement<DropStream> injected = deleteInjector.inject(dropStatement);

    // Then:
    assertThat(injected, is(sameInstance(dropStatement)));
    verifyNoMoreInteractions(topicClient);
  }

  @Test
  public void shouldDropTheDeleteTopicClause() {
    // When:
    final ConfiguredStatement<DropStream> injected = deleteInjector.inject(dropStatement);

    // Then:
    assertThat(injected.getStatementText(), is("DROP STREAM SOMETHING;"));
    assertThat("expected !isDeleteTopic", !injected.getStatement().isDeleteTopic());
  }

  @Test
  public void shouldDeleteTopic() {
    // When:
    deleteInjector.inject(dropStatement);

    // Then:
    verify(topicClient).deleteTopics(ImmutableList.of("something"));
  }

  @Test
  public void shouldDeleteSchemaInSR() throws IOException, RestClientException {
    // Given:
    when(source.isSerdeFormat(DataSourceSerDe.AVRO)).thenReturn(true);

    // When:
    deleteInjector.inject(dropStatement);

    // Then:
    verify(registryClient).deleteSubject("something" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  @Test
  public void shouldNotDeleteSchemaInSRIfNotAvro() throws IOException, RestClientException {
    // When:
    deleteInjector.inject(dropStatement);

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

  private <T extends Statement> ConfiguredStatement<T> givenStatement(
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