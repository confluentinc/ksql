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
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactories;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFactories;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.topic.TopicFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.WindowedSerdes;

/**
 * Base class of create table/stream command
 */
abstract class CreateSourceCommand implements DdlCommand {

  final String sqlExpression;
  final String sourceName;
  final LogicalSchema schema;
  final KeyField keyField;
  final SerdeFactory<?> keySerdeFactory;
  final TimestampExtractionPolicy timestampExtractionPolicy;
  private final Set<SerdeOption> serdeOptions;
  private final KsqlTopic topic;

  CreateSourceCommand(
      final String sqlExpression,
      final CreateSource statement,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient
  ) {
    this(
        sqlExpression,
        statement,
        ksqlConfig,
        kafkaTopicClient,
        SerdeOptions::buildForCreateStatement,
        new KsqlSerdeFactories()
    );
  }

  @VisibleForTesting
  CreateSourceCommand(
      final String sqlExpression,
      final CreateSource statement,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final SerdeOptionsSupplier serdeOptionsSupplier,
      final SerdeFactories serdeFactories
  ) {
    this.sqlExpression = sqlExpression;
    this.sourceName = statement.getName().getSuffix();
    this.topic = buildTopic(statement.getProperties(), kafkaTopicClient);
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

    this.timestampExtractionPolicy = buildTimestampExtractor(statement.getProperties(), schema);

    this.serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        ksqlConfig
    );

    final PhysicalSchema physicalSchema = PhysicalSchema.from(schema, serdeOptions);

    this.keySerdeFactory = extractKeySerde(statement.getProperties());

    final KsqlSerdeFactory valueSerdeFactory = serdeFactories.create(
        topic.getValueFormat().getFormatInfo().getFormat(),
        topic.getValueFormat().getFormatInfo().getAvroFullSchemaName()
    );

    valueSerdeFactory.validate(physicalSchema.valueSchema());
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
      final KafkaTopicClient kafkaTopicClient
  ) {
    final String kafkaTopicName = properties.getKafkaTopic();
    if (!kafkaTopicClient.isTopicExists(kafkaTopicName)) {
      throw new KsqlException("Kafka topic does not exist: " + kafkaTopicName);
    }

    return TopicFactory.create(properties);
  }

  @SuppressWarnings({"unchecked", "OptionalGetWithoutIsPresent"})
  private static SerdeFactory<?> extractKeySerde(
      final CreateSourceProperties properties
  ) {
    final Optional<WindowType> windowType = properties.getWindowType();
    final Optional<Long> windowSize = properties.getWindowSize()
        .map(Duration::toMillis);

    if (!windowType.isPresent()) {
      return (SerdeFactory) Serdes::String;
    }

    if (windowType.get() == WindowType.SESSION) {
      return () -> (Serde) WindowedSerdes.sessionWindowedSerdeFrom(String.class);
    }

    return () -> (Serde) WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.get());
  }

  private static TimestampExtractionPolicy buildTimestampExtractor(
      final CreateSourceProperties properties,
      final LogicalSchema schema
  ) {
    final Optional<String> timestampName = properties.getTimestampColumnName();
    final Optional<String> timestampFormat = properties.getTimestampFormat();
    return TimestampExtractionPolicyFactory
        .create(schema, timestampName, timestampFormat);
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
