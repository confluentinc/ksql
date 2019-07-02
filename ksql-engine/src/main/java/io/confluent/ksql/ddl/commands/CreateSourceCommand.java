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
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateSourceProperties;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.serde.KsqlSerdeFactories;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.SerdeFactories;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Base class of create table/stream command
 */
abstract class CreateSourceCommand implements DdlCommand {

  final String sqlExpression;
  final String sourceName;
  final String topicName;
  final LogicalSchema schema;
  final KeyField keyField;
  private final KafkaTopicClient kafkaTopicClient;
  final SerdeFactory<?> keySerdeFactory;
  final TimestampExtractionPolicy timestampExtractionPolicy;
  private final Set<SerdeOption> serdeOptions;
  private final CreateSourceProperties properties;
  private KsqlSerdeFactory valueSerdeFactory;

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
    this.kafkaTopicClient = kafkaTopicClient;
    this.properties = statement.getProperties();

    checkTopicExists(properties);
    this.topicName = this.sourceName;

    this.schema = buildSchema(statement.getElements());

    if (properties.getKeyField().isPresent()) {
      final String name = properties.getKeyField().get().toUpperCase();

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

    final Optional<String> timestampName = properties.getTimestampName();
    final Optional<String> timestampFormat = properties.getTimestampFormat();
    this.timestampExtractionPolicy = TimestampExtractionPolicyFactory
        .create(schema, timestampName, timestampFormat);

    this.keySerdeFactory = extractKeySerde(properties);
    this.valueSerdeFactory = serdeFactories.create(properties.getValueFormat(), properties);
    this.serdeOptions = serdeOptionsSupplier.build(schema, properties, ksqlConfig);
  }

  Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  private static LogicalSchema buildSchema(final TableElements tableElements) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KsqlException("The statement does not define any columns.");
    }

    final SchemaBuilder valueSchema = SchemaBuilder.struct();
    for (final TableElement tableElement : tableElements) {
      final String fieldName = tableElement.getName();
      final Schema fieldSchema = SchemaConverters.sqlToLogicalConverter()
          .fromSqlType(tableElement.getType().getSqlType());

      valueSchema.field(fieldName, fieldSchema);
    }

    return LogicalSchema.of(valueSchema.build());
  }

  static void checkMetaData(
      final MetaStore metaStore,
      final String sourceName,
      final String topicName
  ) {
    if (metaStore.getSource(sourceName) != null) {
      throw new KsqlException(String.format("Source already exists: %s", sourceName));
    }

    if (metaStore.getTopic(topicName) == null) {
      throw new KsqlException(
          String.format("The corresponding topic does not exist: %s", topicName));
    }
  }

  private void checkTopicExists(final CreateSourceProperties properties) {
    final String kafkaTopicName = properties.getKafkaTopic();
    if (!kafkaTopicClient.isTopicExists(kafkaTopicName)) {
      throw new KsqlException("Kafka topic does not exist: " + kafkaTopicName);
    }
  }

  protected void registerTopic(final MutableMetaStore metaStore, final String entity) {
    if (metaStore.getTopic(topicName) != null) {
      final String errorMessage =
          String.format("Cannot create %s '%s': A %s with name '%s' already exists",
              entity, topicName, entity, topicName);

      throw new KsqlException(errorMessage);
    }

    final String kafkaTopicName = properties.getKafkaTopic();
    final KsqlTopic ksqlTopic = new KsqlTopic(topicName, kafkaTopicName, valueSerdeFactory, false);

    metaStore.putTopic(ksqlTopic);
  }

  private static SerdeFactory<?> extractKeySerde(
      final CreateSourceProperties properties
  ) {
    final Optional<SerdeFactory<Windowed<String>>> windowType = properties.getWindowType();

    //noinspection OptionalIsPresent - <?> causes confusion here
    if (!windowType.isPresent()) {
      return (SerdeFactory) Serdes::String;
    }

    return windowType.get();
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    Set<SerdeOption> build(
        LogicalSchema schema,
        CreateSourceProperties properties,
        KsqlConfig ksqlConfig
    );
  }
}
