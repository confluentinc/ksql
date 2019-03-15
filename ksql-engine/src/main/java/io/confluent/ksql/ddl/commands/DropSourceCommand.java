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

import static io.confluent.ksql.util.ExecutorUtil.RetryBehaviour.ALWAYS;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.AbstractStreamDropStatement;
import io.confluent.ksql.schema.registry.SchemaRegistryUtil;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;

public class DropSourceCommand implements DdlCommand {

  private final String sourceName;
  private final boolean ifExists;
  private final DataSource.DataSourceType dataSourceType;
  private final KafkaTopicClient kafkaTopicClient;
  private final SchemaRegistryClient schemaRegistryClient;
  private final boolean deleteTopic;

  public DropSourceCommand(
      final AbstractStreamDropStatement statement,
      final DataSource.DataSourceType dataSourceType,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient,
      final boolean deleteTopic) {

    this.sourceName = statement.getName().getSuffix();
    this.ifExists = statement.getIfExists();
    this.dataSourceType = dataSourceType;
    this.kafkaTopicClient = kafkaTopicClient;
    this.schemaRegistryClient = schemaRegistryClient;
    this.deleteTopic = deleteTopic;
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    final StructuredDataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      if (ifExists) {
        return new DdlCommandResult(true, "Source " + sourceName + " does not exist.");
      }
      throw new KsqlException("Source " + sourceName + " does not exist.");
    }
    if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KsqlException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType() == DataSource.DataSourceType.KSTREAM ? "STREAM" : "TABLE",
          dataSourceType == DataSource.DataSourceType.KSTREAM ? "STREAM" : "TABLE"
      ));
    }
    final DropTopicCommand dropTopicCommand =
        new DropTopicCommand(dataSource.getKsqlTopicName());
    metaStore.deleteSource(sourceName);
    dropTopicCommand.run(metaStore);

    deleteTopicIfNeeded(dataSource);

    return new DdlCommandResult(
        true,
        "Source " + sourceName + " was dropped. "
            + (deleteTopic ? "Topic '"
            + dataSource.getKafkaTopicName()
            + "' was marked for deletion. Actual deletion "
            + "and removal from brokers may take some time "
            + "to complete." : ""));
  }

  private void deleteTopicIfNeeded(final StructuredDataSource dataSource) {
    if (!deleteTopic) {
      return;
    }

    try {
      final List<String> topic = Collections
          .singletonList(dataSource.getKafkaTopicName());

      ExecutorUtil.executeWithRetries(() -> kafkaTopicClient.deleteTopics(topic), ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException("Could not delete the corresponding kafka topic: "
          + dataSource.getKafkaTopicName(), e);
    }

    if (dataSource.isSerdeFormat(DataSource.DataSourceSerDe.AVRO)) {
      try {
        SchemaRegistryUtil.deleteSubjectWithRetries(
            schemaRegistryClient,
            dataSource.getKafkaTopicName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
      } catch (final Exception e) {
        throw new KsqlException("Could not clean up the schema registry for topic: "
            + dataSource.getKafkaTopicName(), e);
      }
    }
  }
}