/**
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

import java.util.Collections;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.tree.AbstractStreamDropStatement;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;


public class DropSourceCommand implements DdlCommand {

  private static final int NUM_RETRIES = 5;
  private static final int RETRY_BACKOFF_MS = 500;

  private final String sourceName;
  private final DataSource.DataSourceType dataSourceType;
  private final KafkaTopicClient kafkaTopicClient;
  private final SchemaRegistryClient schemaRegistryClient;
  private final boolean withTopic;

  public DropSourceCommand(
      final AbstractStreamDropStatement statement,
      final DataSource.DataSourceType dataSourceType,
      final KafkaTopicClient kafkaTopicClient,
      final SchemaRegistryClient schemaRegistryClient,
      final boolean withTopic) {

    this.sourceName = statement.getName().getSuffix();
    this.dataSourceType = dataSourceType;
    this.kafkaTopicClient = kafkaTopicClient;
    this.schemaRegistryClient = schemaRegistryClient;
    this.withTopic = withTopic;
  }

  @Override
  public DdlCommandResult run(MetaStore metaStore, boolean isValidatePhase) {
    StructuredDataSource dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      throw new KsqlException("Source " + sourceName + " does not exist.");
    }
    if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KsqlException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType() == DataSource.DataSourceType.KSTREAM ? "STREAM" : "TABLE",
          dataSourceType == DataSource.DataSourceType.KSTREAM ? "STREAM" : "TABLE"
      ));
    }
    DropTopicCommand dropTopicCommand =
        new DropTopicCommand(dataSource.getKsqlTopic().getTopicName());
    metaStore.deleteSource(sourceName);
    dropTopicCommand.run(metaStore, isValidatePhase);

    deleteTopicIfNeeded(dataSource, isValidatePhase);

    return new DdlCommandResult(true, "Source " + sourceName + " was dropped. "
                                      + (withTopic ? "Topic '"
                                                    + dataSource.getKsqlTopic().getTopicName()
                                                    + "' was marked for deletion. Actual deletion "
                                                    + "and removal from brokers may take some time "
                                                    + "to complete." : ""));
  }

  private void deleteTopicIfNeeded(StructuredDataSource dataSource, boolean isValidatePhase) {
    if (!isValidatePhase && withTopic) {
      int retries = 0;
      while (retries < NUM_RETRIES) {
        try {
          if (retries != 0) {
            Thread.sleep(RETRY_BACKOFF_MS);
          }
          kafkaTopicClient.deleteTopics(
              Collections.singletonList(dataSource.getKsqlTopic().getKafkaTopicName()));
          break;
        } catch (Exception e) {
          retries++;
          if (retries >= NUM_RETRIES) {
            throw new KsqlException("Could not delete the corresponding kafka topic: "
                                    + dataSource.getKsqlTopic().getKafkaTopicName(), e);
          }
        }
      }

      if (dataSource.getKsqlTopic().getKsqlTopicSerDe().getSerDe()
          == DataSource.DataSourceSerDe.AVRO) {
        retries = 0;
        while (retries < NUM_RETRIES) {
          try {
            if (retries != 0) {
              Thread.sleep(RETRY_BACKOFF_MS);
            }
            schemaRegistryClient
                .deleteSubject(sourceName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
            break;
          } catch (Exception e) {
            retries++;
            if (retries >= NUM_RETRIES) {
              throw new KsqlException("Could not clean up the schema registry for topic: "
                                      + sourceName, e);
            }
          }
        }
      }
    }
  }
}