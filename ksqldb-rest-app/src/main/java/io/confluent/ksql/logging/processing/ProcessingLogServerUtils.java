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

package io.confluent.ksql.logging.processing;

import io.confluent.common.logging.LogRecordStructBuilder;
import io.confluent.ksql.exception.KafkaTopicExistsException;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.IdentifierUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProcessingLogServerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingLogServerUtils.class);
  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(w -> !IdentifierUtil.isValid(w), Option.AS_COLUMN_LIST);

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
    return ReservedInternalTopics.processingLogTopic(config, ksqlConfig);
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

  public static String processingLogStreamCreateStatement(
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig
  ) {
    return processingLogStreamCreateStatement(
        config.getString(ProcessingLogConfig.STREAM_NAME),
        getTopicName(config, ksqlConfig)
    );
  }

  private static String processingLogStreamCreateStatement(
      final String name,
      final String topicName
  ) {
    final Schema schema = getMessageSchema();

    final String elements = FORMATTER.format(schema);

    return "CREATE STREAM " + name
        + " (" + elements + ")"
        + " WITH(KAFKA_TOPIC='" + topicName + "', "
        + "VALUE_FORMAT='JSON', KEY_FORMAT='KAFKA'"
        + ");";
  }
}
