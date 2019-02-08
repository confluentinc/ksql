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

package io.confluent.ksql.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropSourceCommandTest {

  private static final String STREAM_NAME = "foo";
  private static final String TOPIC_NAME = "foo_topic";

  @Mock
  private MetaStore metaStore;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private StructuredDataSource dataSource;
  @Mock
  private KsqlTopic ksqlTopic;

  private DropSourceCommand dropSourceCommand;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    when(metaStore.getSource(STREAM_NAME)).thenReturn(dataSource);
    when(dataSource.getKsqlTopic()).thenReturn(ksqlTopic);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(ksqlTopic.getKafkaTopicName()).thenReturn(TOPIC_NAME);
  }

  @Test
  public void shouldSucceedOnMissingSourceWithIfExists() {
    // Given:
    givenDropSourceCommand(true, true);
    givenSourceDoesNotExist();

    // When:
    final DdlCommandResult result = dropSourceCommand.run(metaStore);

    // Then:
    assertThat(result.getMessage(), equalTo("Source foo does not exist."));
  }

  @Test
  public void shouldFailOnMissingSourceWithNoIfExists() {
    // Given:
    givenDropSourceCommand(false, true);
    givenSourceDoesNotExist();

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Source foo does not exist.");

    // When:
    dropSourceCommand.run(metaStore);
  }

  @Test
  public void shouldDeleteTopicIfDeleteTopicTrue() {
    // Given:
    givenDropSourceCommand(false, true);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList(TOPIC_NAME));
  }

  @Test
  public void shouldNotDeleteTopicIfDeleteTopicFalse() {
    // Given:
    givenDropSourceCommand(false, false);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(kafkaTopicClient, never()).deleteTopics(any());
  }

  @Test
  public void shouldCleanUpSchemaIfAvroTopic() throws Exception {
    // Given:
    when(dataSource.isAvroSerialized()).thenReturn(true);
    givenDropSourceCommand(false, true);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(schemaRegistryClient).deleteSubject(STREAM_NAME + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  @Test
  public void shouldNotCleanUpSchemaIfNonAvroTopic() throws Exception {
    // Given:
    when(dataSource.isAvroSerialized()).thenReturn(false);
    givenDropSourceCommand(false, true);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(schemaRegistryClient, never()).deleteSubject(anyString());
  }

  private void givenDropSourceCommand(final boolean ifExists, final boolean deleteTopic) {
    dropSourceCommand = new DropSourceCommand(
        new DropStream(QualifiedName.of(STREAM_NAME), ifExists, deleteTopic),
        StructuredDataSource.DataSourceType.KSTREAM,
        kafkaTopicClient,
        schemaRegistryClient,
        deleteTopic);
  }

  private void givenSourceDoesNotExist() {
    when(metaStore.getSource(STREAM_NAME)).thenReturn(null);
  }
}
