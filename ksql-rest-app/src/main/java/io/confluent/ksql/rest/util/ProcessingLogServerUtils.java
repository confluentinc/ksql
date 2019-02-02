/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.TypeUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProcessingLogServerUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingLogServerUtils.class);

  private ProcessingLogServerUtils() {
  }

  public static Schema getMessageSchema() {
    return new LogRecordStructBuilder()
        .withMessageSchemaAndValue(
            new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, null))
        .build()
        .schema();
  }

  private static String getTopicName(
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

  public static void maybeCreateProcessingLogTopic(
      final KafkaTopicClient topicClient,
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig) {
    if (!config.getString(ProcessingLogConfig.TOPIC_AUTO_CREATE).equals(
        ProcessingLogConfig.AUTO_CREATE_ON)) {
      return;
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
  }

  public static String processingLogStreamCreateStatement(
      final String name,
      final String topicName) {
    final Schema schema = getMessageSchema();
    final String statementNoSchema =
        String.format(
            "CREATE STREAM %s WITH(KAFKA_TOPIC='%s', VALUE_FORMAT='JSON');", name, topicName);
    final PreparedStatement preparedStatement = new KsqlParser().buildAst(
        statementNoSchema,
        new MetaStoreImpl(new InternalFunctionRegistry()),
        s -> {}).get(0);
    final AbstractStreamCreateStatement streamCreateStatement
        = (AbstractStreamCreateStatement) preparedStatement.getStatement();
    return SqlFormatter.formatSql(
        streamCreateStatement.copyWith(
            TypeUtil.buildTableElementsForSchema(schema),
            streamCreateStatement.getProperties()));
  }
}
