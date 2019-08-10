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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.TopicFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Optional;
import java.util.Set;

/**
 * Base class of create table/stream command
 */
abstract class CreateSourceCommand implements DdlCommand {

  final String sqlExpression;
  final String sourceName;
  final LogicalSchema schema;
  final KeyField keyField;
  final TimestampExtractionPolicy timestampExtractionPolicy;
  private final Set<SerdeOption> serdeOptions;
  private final KsqlTopic topic;

  CreateSourceCommand(
      final String sqlExpression,
      final CreateSource statement,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext
  ) {
    this(
        sqlExpression,
        statement,
        ksqlConfig,
        serviceContext,
        SerdeOptions::buildForCreateStatement,
        new GenericRowSerDe()
    );
  }

  @VisibleForTesting
  CreateSourceCommand(
      final String sqlExpression,
      final CreateSource statement,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final SerdeOptionsSupplier serdeOptionsSupplier,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this.sqlExpression = sqlExpression;
    this.sourceName = statement.getName().getSuffix();
    this.topic = buildTopic(statement.getProperties(), serviceContext);
    this.schema = buildSchema(statement.getElements());

    if (statement.getProperties().getKeyField().isPresent()) {
      final String name = statement.getProperties().getKeyField().get().toUpperCase();

      final String keyFieldName = StringUtil.cleanQuotes(name);
      final Field keyField = schema.findValueField(keyFieldName)
          .orElseThrow(() -> new KsqlException(
              "The KEY column set in the WITH clause does not exist in the schema: '"
                  + keyFieldName + "'"
          ));

      this.keyField = KeyField.of(keyFieldName, keyField);
    } else {
      this.keyField = KeyField.none();
    }

    this.timestampExtractionPolicy = buildTimestampExtractor(
        ksqlConfig,
        statement.getProperties(),
        schema
    );

    this.serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        ksqlConfig
    );

    validateSerdeCanHandleSchemas(
        ksqlConfig,
        serviceContext,
        valueSerdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
  }

  Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  KsqlTopic getTopic() {
    return topic;
  }

  private static LogicalSchema buildSchema(final TableElements tableElements) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KsqlException("The statement does not define any columns.");
    }

    return tableElements.toLogicalSchema();
  }

  private static KsqlTopic buildTopic(
      final CreateSourceProperties properties,
      final ServiceContext serviceContext
  ) {
    final String kafkaTopicName = properties.getKafkaTopic();
    if (!serviceContext.getTopicClient().isTopicExists(kafkaTopicName)) {
      throw new KsqlException("Kafka topic does not exist: " + kafkaTopicName);
    }

    return TopicFactory.create(properties);
  }

  private static TimestampExtractionPolicy buildTimestampExtractor(
      final KsqlConfig ksqlConfig,
      final CreateSourceProperties properties,
      final LogicalSchema schema
  ) {
    final Optional<String> timestampName = properties.getTimestampColumnName();
    final Optional<String> timestampFormat = properties.getTimestampFormat();
    return TimestampExtractionPolicyFactory
        .create(ksqlConfig, schema, timestampName, timestampFormat);
  }

  private static void validateSerdeCanHandleSchemas(
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ValueSerdeFactory valueSerdeFactory,
      final PhysicalSchema physicalSchema,
      final KsqlTopic topic
  ) {
    valueSerdeFactory.create(
        topic.getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    ).close();
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    Set<SerdeOption> build(
        LogicalSchema schema,
        Format valueFormat,
        Optional<Boolean> wrapSingleValues,
        KsqlConfig ksqlConfig
    );
  }
}
