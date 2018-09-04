/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.ddl.commands;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.AbstractStreamDropStatement;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;

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
  public DdlCommandResult run(final MetaStore metaStore, final boolean isValidatePhase) {
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
        new DropTopicCommand(dataSource.getKsqlTopic().getTopicName());
    metaStore.deleteSource(sourceName);
    dropTopicCommand.run(metaStore, isValidatePhase);

    deleteTopicIfNeeded(dataSource, isValidatePhase);

    return new DdlCommandResult(true, "Source " + sourceName + " was dropped. "
                                      + (deleteTopic ? "Topic '"
                                                    + dataSource.getKsqlTopic().getKafkaTopicName()
                                                    + "' was marked for deletion. Actual deletion "
                                                    + "and removal from brokers may take some time "
                                                    + "to complete." : ""));
  }

  private void deleteTopicIfNeeded(
      final StructuredDataSource dataSource,
      final boolean isValidatePhase) {
    if (!isValidatePhase && deleteTopic) {
      try {
        ExecutorUtil.executeWithRetries(
            () -> kafkaTopicClient.deleteTopics(
                    Collections.singletonList(
                        dataSource.getKsqlTopic().getKafkaTopicName())),
            ExecutorUtil.RetryBehaviour.ALWAYS);
      } catch (final Exception e) {
        throw new KsqlException("Could not delete the corresponding kafka topic: "
            + dataSource.getKsqlTopic().getKafkaTopicName(), e);
      }
      if (dataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe()
          == DataSource.DataSourceSerDe.AVRO) {
        try {
          ExecutorUtil.executeWithRetries(
              () -> schemaRegistryClient.deleteSubject(
                  sourceName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX),
              ExecutorUtil.RetryBehaviour.ALWAYS);
        } catch (final Exception e) {
          throw new KsqlException("Could not clean up the schema registry for topic: "
              + sourceName, e);
        }
      }
    }
  }
}