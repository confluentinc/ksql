package io.confluent.ksql.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import org.easymock.EasyMock;
import org.junit.Test;

public class DropSourceCommandTest {
  MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  @Test
  public void shouldSucceedOnMissingSourceWithIfExists() {
    final DropSourceCommand dropSourceCommand = new DropSourceCommand(
        new DropStream(QualifiedName.of("foo"), true, true), StructuredDataSource.DataSourceType
        .KSTREAM,
        new FakeKafkaTopicClient(),
        EasyMock.niceMock(SchemaRegistryClient.class), true);
    final DdlCommandResult result = dropSourceCommand.run(metaStore, false);
    assertThat(result.getMessage(), equalTo("Source foo does not exist."));
  }

  @Test
  public void shouldFailOnMissingSourceWithNoIfExists() {
    final DropSourceCommand dropSourceCommand = new DropSourceCommand(
        new DropStream(QualifiedName.of("foo"), false, false), StructuredDataSource.DataSourceType
        .KSTREAM,
        new FakeKafkaTopicClient(),
        EasyMock.niceMock(SchemaRegistryClient.class), true);
    try {
      dropSourceCommand.run(metaStore, false);
      fail("Should raise a Ksql Exception if source not found");
    } catch (final KsqlException e) {}
  }
}
