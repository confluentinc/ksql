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

package io.confluent.ksql.ddl.commands;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlTopicSerDe;
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
  private static final boolean WITH_DELETE_TOPIC = true;
  private static final boolean WITHOUT_DELETE_TOPIC = false;
  private static final boolean IF_EXISTS = true;
  private static final boolean ALWAYS = false;

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private DataSource<?> dataSource;
  @Mock
  private KsqlTopicSerDe ksqlTopicSerDe;

  private DropSourceCommand dropSourceCommand;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(metaStore.getSource(STREAM_NAME)).thenReturn((DataSource)dataSource);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(dataSource.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(dataSource.getKsqlTopicSerde()).thenReturn(ksqlTopicSerDe);
    when(ksqlTopicSerDe.getSerDe()).thenReturn(Format.JSON);
  }

  @Test
  public void shouldSucceedOnMissingSourceWithIfExists() {
    // Given:
    givenDropSourceCommand(IF_EXISTS, WITH_DELETE_TOPIC);
    givenSourceDoesNotExist();

    // When:
    final DdlCommandResult result = dropSourceCommand.run(metaStore);

    // Then:
    assertThat(result.getMessage(), equalTo("Source foo does not exist."));
  }

  @Test
  public void shouldFailOnMissingSourceWithNoIfExists() {
    // Given:
    givenDropSourceCommand(ALWAYS, WITH_DELETE_TOPIC);
    givenSourceDoesNotExist();

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Source foo does not exist.");

    // When:
    dropSourceCommand.run(metaStore);
  }

  @Test
  public void shouldFailOnDropIncompatibleSource() {
    // Given:
    givenIncompatibleDropSourceCommand();

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Incompatible data source type is STREAM");

    // When:
    dropSourceCommand.run(metaStore);
  }

  @Test
  public void shouldDeleteTopicIfDeleteTopicTrue() {
    // Given:
    givenDropSourceCommand(ALWAYS, WITH_DELETE_TOPIC);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(kafkaTopicClient).deleteTopics(Collections.singletonList(TOPIC_NAME));
  }

  @Test
  public void shouldNotDeleteTopicIfDeleteTopicFalse() {
    // Given:
    givenDropSourceCommand(ALWAYS, WITHOUT_DELETE_TOPIC);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(kafkaTopicClient, never()).deleteTopics(any());
  }

  @Test
  public void shouldCleanUpSchemaIfAvroTopic() throws Exception {
    // Given:
    when(ksqlTopicSerDe.getSerDe()).thenReturn(Format.AVRO);
    givenDropSourceCommand(ALWAYS, WITH_DELETE_TOPIC);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(schemaRegistryClient)
        .deleteSubject(TOPIC_NAME + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  @Test
  public void shouldNotCleanUpSchemaIfNonAvroTopic() throws Exception {
    // Given:
    when(ksqlTopicSerDe.getSerDe()).thenReturn(Format.DELIMITED);
    givenDropSourceCommand(ALWAYS, WITH_DELETE_TOPIC);

    // When:
    dropSourceCommand.run(metaStore);

    // Then:
    verify(schemaRegistryClient, never()).deleteSubject(anyString());
  }

  private void givenDropSourceCommand(final boolean ifExists, final boolean deleteTopic) {
    dropSourceCommand = new DropSourceCommand(
        new DropStream(QualifiedName.of(STREAM_NAME), ifExists, deleteTopic),
        DataSourceType.KSTREAM,
        kafkaTopicClient,
        schemaRegistryClient,
        deleteTopic);
  }

  private void givenIncompatibleDropSourceCommand() {
    dropSourceCommand = new DropSourceCommand(
        new DropStream(QualifiedName.of(STREAM_NAME), true, true),
        DataSourceType.KTABLE,
        kafkaTopicClient,
        schemaRegistryClient,
        true);
  }

  private void givenSourceDoesNotExist() {
    when(metaStore.getSource(STREAM_NAME)).thenReturn(null);
  }
}
