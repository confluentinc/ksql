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

import java.util.Map;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;

public class CreateTableCommand extends AbstractCreateStreamCommand {

  private String stateStoreName;

  public CreateTableCommand(
      String sqlExpression,
      CreateTable createTable,
      KafkaTopicClient kafkaTopicClient,
      boolean enforceTopicExistence
  ) {
    super(sqlExpression,
          createTable,
        kafkaTopicClient,
          enforceTopicExistence);

    Map<String, Expression> properties = createTable.getProperties();

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
  public DdlCommandResult run(MetaStore metaStore, boolean isValidatePhase) {
    if (registerTopicCommand != null) {
      registerTopicCommand.run(metaStore, isValidatePhase);
    }
    checkMetaData(metaStore, sourceName, topicName);
    KsqlTable ksqlTable = new KsqlTable(
        sqlExpression,
        sourceName,
        schema,
        (keyColumnName.length() == 0)
          ? null : SchemaUtil.getFieldByName(schema, keyColumnName).orElse(null),
        timestampExtractionPolicy,
        metaStore.getTopic(topicName),
        stateStoreName, isWindowed
    );

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putSource(ksqlTable.cloneWithTimeKeyColumns());
    return new DdlCommandResult(true, "Table created");
  }

}
