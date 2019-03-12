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

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;
import java.util.Map;
import java.util.Optional;

public class CreateTableCommand extends AbstractCreateStreamCommand {

  private final String stateStoreName;

  CreateTableCommand(
      final String sqlExpression,
      final CreateTable createTable,
      final KafkaTopicClient kafkaTopicClient
  ) {
    super(sqlExpression, createTable, kafkaTopicClient);

    final Map<String, Expression> properties = createTable.getProperties();

    if (!properties.containsKey(DdlConfig.KEY_NAME_PROPERTY)) {
      throw new KsqlException(
          "Cannot define a TABLE without providing the KEY column name in the WITH clause."
      );
    }

    if (properties.containsKey(DdlConfig.STATE_STORE_NAME_PROPERTY)) {
      this.stateStoreName = StringUtil.cleanQuotes(
          properties.get(DdlConfig.STATE_STORE_NAME_PROPERTY).toString()
      );
    } else {
      this.stateStoreName = createTable.getName().toString() + "_statestore";
    }
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
        schema,
        (keyColumnName.isEmpty())
          ? Optional.empty() : SchemaUtil.getFieldByName(schema, keyColumnName),
        timestampExtractionPolicy,
        metaStore.getTopic(topicName),
        stateStoreName, keySerdeFactory
    );

    metaStore.putSource(ksqlTable.cloneWithTimeKeyColumns());
    return new DdlCommandResult(true, "Table created");
  }
}
