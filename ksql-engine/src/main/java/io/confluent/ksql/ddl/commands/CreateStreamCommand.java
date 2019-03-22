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
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;

public class CreateStreamCommand extends AbstractCreateStreamCommand {

  public CreateStreamCommand(
      final String sqlExpression,
      final CreateStream createStream,
      final KafkaTopicClient kafkaTopicClient
  ) {
    super(sqlExpression, createStream, kafkaTopicClient);
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    if (registerTopicCommand != null) {
      try {
        registerTopicCommand.run(metaStore);
      } catch (KsqlException e) {
        final String errorMessage =
                String.format("Cannot create stream '%s': %s", topicName, e.getMessage());
        throw new KsqlException(errorMessage, e);
      }
    }
    checkMetaData(metaStore, sourceName, topicName);
    final KsqlStream ksqlStream = new KsqlStream<>(
        sqlExpression,
        sourceName,
        schema,
        (keyColumnName.length() == 0)
          ? Optional.empty() : SchemaUtil.getFieldByName(schema, keyColumnName),
        timestampExtractionPolicy,
        metaStore.getTopic(topicName),
        keySerdeFactory
    );

    metaStore.putSource(ksqlStream.cloneWithTimeKeyColumns());
    return new DdlCommandResult(true, "Stream created");
  }
}
