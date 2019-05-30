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

import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

public class CreateTableCommand extends CreateSourceCommand {

  CreateTableCommand(
      final String sqlExpression,
      final CreateTable createTable,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient
  ) {
    super(sqlExpression, createTable, ksqlConfig, kafkaTopicClient);
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    if (registerTopicCommand != null) {
      try {
        registerTopicCommand.run(metaStore);
      } catch (KsqlException e) {
        final String errorMessage =
                String.format("Cannot create table '%s': %s", topicName, e.getMessage());
        throw new KsqlException(errorMessage, e);
      }
    }
    checkMetaData(metaStore, sourceName, topicName);

    final KsqlTable ksqlTable = new KsqlTable<>(
        sqlExpression,
        sourceName,
        schema.withImplicitFields(),
        getSerdeOptions(),
        keyField,
        timestampExtractionPolicy,
        metaStore.getTopic(topicName),
        keySerdeFactory
    );

    metaStore.putSource(ksqlTable);
    return new DdlCommandResult(true, "Table created");
  }
}
