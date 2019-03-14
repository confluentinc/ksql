package io.confluent.ksql.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import org.easymock.EasyMock;
import org.junit.Test;

public class DropSourceCommandTest {

  private static final String STREAM_NAME = "foo";
  private static final String KSQL_TOPIC_NAME = "FOO";
  private static final String KAFKA_TOPIC_NAME = "foo_topic";

  private final MetaStore metaStore = EasyMock.mock(MetaStore.class);
  private final KafkaTopicClient kafkaTopicClient = EasyMock.niceMock(KafkaTopicClient.class);
  private final SchemaRegistryClient schemaRegistryClient = EasyMock.mock(SchemaRegistryClient.class);
  private final StructuredDataSource dataSource = EasyMock.mock(StructuredDataSource.class);
  private final KsqlTopic ksqlTopic = EasyMock.mock(KsqlTopic.class);
  private final KsqlTopicSerDe ksqlTopicSerDe = EasyMock.mock(KsqlTopicSerDe.class);

  private DropSourceCommand dropSourceCommand;

  @Test
  public void shouldSucceedOnMissingSourceWithIfExists() {
    // Given:
    givenDropSourceCommand(true);

    EasyMock.expect(metaStore.getSource(STREAM_NAME)).andReturn(null);
    EasyMock.replay(metaStore);

    // When:
    final DdlCommandResult result = dropSourceCommand.run(metaStore, false);

    // Then:
    assertThat(result.getMessage(), equalTo("Source foo does not exist."));
  }

  @Test
  public void shouldFailOnMissingSourceWithNoIfExists() {
    // Given:
    givenDropSourceCommand(false);

    EasyMock.expect(metaStore.getSource(STREAM_NAME)).andReturn(null);
    EasyMock.replay(metaStore);

    // When/Then:
    try {
      dropSourceCommand.run(metaStore, false);
      fail("Should raise a Ksql Exception if source not found");
    } catch (final KsqlException e) {
    }
  }

  @Test
  public void shouldCleanUpSchemaIfAvroTopic() throws Exception {
    // Given:
    givenDropSourceCommand(true);

    EasyMock.expect(metaStore.getSource(STREAM_NAME)).andReturn(dataSource);
    metaStore.deleteSource(STREAM_NAME);
    EasyMock.expectLastCall();
    metaStore.deleteTopic(KSQL_TOPIC_NAME);
    EasyMock.expectLastCall();

    EasyMock.expect(dataSource.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(dataSource.getKsqlTopic()).andReturn(ksqlTopic).anyTimes();

    EasyMock.expect(ksqlTopic.getTopicName()).andReturn(KSQL_TOPIC_NAME);
    EasyMock.expect(ksqlTopic.getKafkaTopicName()).andReturn(KAFKA_TOPIC_NAME).anyTimes();
    EasyMock.expect(ksqlTopic.getKsqlTopicSerDe()).andReturn(ksqlTopicSerDe);

    EasyMock.expect(ksqlTopicSerDe.getSerDe()).andReturn(DataSource.DataSourceSerDe.AVRO);

    EasyMock.expect(schemaRegistryClient.deleteSubject(KAFKA_TOPIC_NAME + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX))
        .andStubReturn(Collections.emptyList());

    EasyMock.replay(metaStore, dataSource, ksqlTopic, ksqlTopicSerDe, schemaRegistryClient);

    // When:
    dropSourceCommand.run(metaStore, false);

    // Then:
    EasyMock.verify(schemaRegistryClient);
  }

  private void givenDropSourceCommand(final boolean ifExists) {
    dropSourceCommand = new DropSourceCommand(
        new DropStream(QualifiedName.of(STREAM_NAME), ifExists, true),
        StructuredDataSource.DataSourceType.KSTREAM,
        kafkaTopicClient,
        schemaRegistryClient,
        true);
  }
}
