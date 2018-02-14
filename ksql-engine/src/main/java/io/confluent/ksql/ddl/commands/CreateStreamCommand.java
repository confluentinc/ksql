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

import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.SchemaUtil;


public class CreateStreamCommand extends AbstractCreateStreamCommand {

  public CreateStreamCommand(
      String sqlExpression,
      CreateStream createStream,
      Map<String, Object> overriddenProperties,
      KafkaTopicClient kafkaTopicClient,
      boolean enforceTopicExistence
  ) {
    super(sqlExpression,
          createStream,
          overriddenProperties,
          kafkaTopicClient,
          enforceTopicExistence);
  }

  @Override
  public DDLCommandResult run(MetaStore metaStore) {
    if (registerTopicCommand != null) {
      registerTopicCommand.run(metaStore);
    }
    checkMetaData(metaStore, sourceName, topicName);
    KsqlStream ksqlStream = new KsqlStream(
        sqlExpression,
        sourceName,
        schema,
        (keyColumnName.length() == 0)
          ? null : SchemaUtil.getFieldByName(schema, keyColumnName).orElse(null),
        (timestampColumnName.length() == 0)
          ? null : SchemaUtil.getFieldByName(schema, timestampColumnName).orElse(null),
        metaStore.getTopic(topicName)
    );

    // TODO: Need to check if the topic exists.
    // Add the topic to the metastore
    metaStore.putSource(ksqlStream.cloneWithTimeKeyColumns());
    return new DDLCommandResult(true, "Stream created");
  }
}
