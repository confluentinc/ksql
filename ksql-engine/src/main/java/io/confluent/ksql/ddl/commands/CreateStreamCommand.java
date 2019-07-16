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
import io.confluent.ksql.util.KsqlConfig;

public class CreateStreamCommand extends CreateSourceCommand {

  CreateStreamCommand(
      final String sqlExpression,
      final CreateStream createStream,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient
  ) {
    super(sqlExpression, createStream, ksqlConfig, kafkaTopicClient);
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {

    final KsqlStream<?> ksqlStream = new KsqlStream<>(
        sqlExpression,
        sourceName,
        schema,
        getSerdeOptions(),
        keyField,
        timestampExtractionPolicy,
        buildTopic(),
        keySerdeFactory
    );

    metaStore.putSource(ksqlStream);

    return new DdlCommandResult(true, "Stream created");
  }
}
