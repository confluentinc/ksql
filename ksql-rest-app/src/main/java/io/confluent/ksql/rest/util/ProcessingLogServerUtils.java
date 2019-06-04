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

package io.confluent.ksql.rest.util;

import io.confluent.common.logging.LogRecordStructBuilder;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProcessingLogServerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingLogServerUtils.class);

  private ProcessingLogServerUtils() {
  }

  static Schema getMessageSchema() {
    return new LogRecordStructBuilder()
        .withMessageSchemaAndValue(
            new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, null))
        .build()
        .schema();
  }

  public static String getTopicName(
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig) {
    final String topicNameConfig = config.getString(ProcessingLogConfig.TOPIC_NAME);
    if (topicNameConfig.equals(ProcessingLogConfig.TOPIC_NAME_NOT_SET)) {
      return String.format(
          "%s%s",
          ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
          ProcessingLogConfig.TOPIC_NAME_DEFAULT_SUFFIX
      );
    } else {
      return topicNameConfig;
    }
  }

  public static Optional<String> maybeCreateProcessingLogTopic(
      final KafkaTopicClient topicClient,
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig) {
    if (!config.getBoolean(ProcessingLogConfig.TOPIC_AUTO_CREATE)) {
      return Optional.empty();
    }
    final String topicName = getTopicName(config, ksqlConfig);
    final int nPartitions =
        config.getInt(ProcessingLogConfig.TOPIC_PARTITIONS);
    final short nReplicas =
        config.getShort(ProcessingLogConfig.TOPIC_REPLICATION_FACTOR);
    try {
      topicClient.createTopic(topicName, nPartitions, nReplicas);
    } catch (final KafkaTopicExistsException e) {
      LOGGER.info(String.format("Log topic %s already exists", topicName), e);
    }
    return Optional.of(topicName);
  }

  public static PreparedStatement<CreateSource> processingLogStreamCreateStatement(
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig
  ) {
    return processingLogStreamCreateStatement(
        config.getString(ProcessingLogConfig.STREAM_NAME),
        getTopicName(config, ksqlConfig)
    );
  }

  private static PreparedStatement<CreateSource> processingLogStreamCreateStatement(
      final String name,
      final String topicName
  ) {
    final Schema schema = getMessageSchema();
    final String statementNoSchema =
        String.format(
            "CREATE STREAM %s WITH(KAFKA_TOPIC='%s', VALUE_FORMAT='JSON');", name, topicName);
    final DefaultKsqlParser parser = new DefaultKsqlParser();
    final ParsedStatement parsed = parser.parse(statementNoSchema).get(0);
    final PreparedStatement<?> preparedStatement = parser
        .prepare(parsed, new MetaStoreImpl(new InternalFunctionRegistry()));

    final CreateSource streamCreateStatement
        = (CreateSource) preparedStatement.getStatement();
    final CreateSource streamCreateStatementWithSchema =
        streamCreateStatement.copyWith(
            TableElement.fromSchema(schema),
            streamCreateStatement.getProperties());
    return PreparedStatement.of(
        SqlFormatter.formatSql(streamCreateStatementWithSchema),
        streamCreateStatementWithSchema);
  }
}
