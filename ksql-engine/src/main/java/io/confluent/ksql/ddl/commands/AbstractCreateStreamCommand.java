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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.SerdeFactory;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;


/**
 * Base class of create table/stream command
 */
abstract class AbstractCreateStreamCommand implements DdlCommand {

  private static final Map<String, SerdeFactory<Windowed<String>>> WINDOW_TYPES = ImmutableMap.of(
      "SESSION", () -> WindowedSerdes.sessionWindowedSerdeFrom(String.class),
      "TUMBLING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class),
      "HOPPING", () -> WindowedSerdes.timeWindowedSerdeFrom(String.class)
  );

  final String sqlExpression;
  final String sourceName;
  final String topicName;
  final Schema schema;
  final KeyField keyField;
  final RegisterTopicCommand registerTopicCommand;
  private final KafkaTopicClient kafkaTopicClient;
  final SerdeFactory<?> keySerdeFactory;
  final TimestampExtractionPolicy timestampExtractionPolicy;

  AbstractCreateStreamCommand(
      final String sqlExpression,
      final AbstractStreamCreateStatement statement,
      final KafkaTopicClient kafkaTopicClient
  ) {
    this.sqlExpression = sqlExpression;
    this.sourceName = statement.getName().getSuffix();
    this.kafkaTopicClient = kafkaTopicClient;

    final Map<String, Expression> properties = statement.getProperties();
    validateWithClause(properties.keySet());

    if (properties.containsKey(DdlConfig.TOPIC_NAME_PROPERTY)
        && !properties.containsKey(DdlConfig.VALUE_FORMAT_PROPERTY)) {
      this.topicName = StringUtil.cleanQuotes(
          properties.get(DdlConfig.TOPIC_NAME_PROPERTY).toString().toUpperCase());

      checkTopicNameNotNull(properties);
      this.registerTopicCommand = null;
    } else {
      this.topicName = this.sourceName;
      this.registerTopicCommand = registerTopicFirst(properties);
    }

    this.schema = getStreamTableSchema(statement.getElements());

    if (properties.containsKey(DdlConfig.KEY_NAME_PROPERTY)) {
      final String name = properties.get(DdlConfig.KEY_NAME_PROPERTY).toString().toUpperCase();

      final String keyFieldName = StringUtil.cleanQuotes(name);
      final Field keyField = SchemaUtil.getFieldByName(schema, keyFieldName)
          .orElseThrow(() -> new KsqlException(
            "No column with the provided key column name in the WITH "
                + "clause, " + keyFieldName + ", exists in the defined schema."
          ));

      this.keyField = KeyField.of(keyFieldName, keyField);
    } else {
      this.keyField = KeyField.none();
    }

    final String timestampName = properties.containsKey(DdlConfig.TIMESTAMP_NAME_PROPERTY)
        ? properties.get(DdlConfig.TIMESTAMP_NAME_PROPERTY).toString()
        : null;
    final String timestampFormat = properties.containsKey(DdlConfig.TIMESTAMP_FORMAT_PROPERTY)
        ? properties.get(DdlConfig.TIMESTAMP_FORMAT_PROPERTY).toString()
        : null;
    this.timestampExtractionPolicy = TimestampExtractionPolicyFactory.create(schema,
        timestampName,
        timestampFormat);

    this.keySerdeFactory = extractKeySerde(properties);
  }

  private static void checkTopicNameNotNull(final Map<String, Expression> properties) {
    // TODO: move the check to grammar
    if (properties.get(DdlConfig.TOPIC_NAME_PROPERTY) == null) {
      throw new KsqlException("Topic name should be set in WITH clause.");
    }
  }

  private static Schema getStreamTableSchema(final List<TableElement> tableElements) {
    if (tableElements.isEmpty()) {
      throw new KsqlException("The statement does not define any columns.");
    }

    SchemaBuilder tableSchema = SchemaBuilder.struct();
    for (final TableElement tableElement : tableElements) {
      if (tableElement.getName().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          || tableElement.getName().equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        throw new KsqlException(
            SchemaUtil.ROWTIME_NAME + "/" + SchemaUtil.ROWKEY_NAME + " are "
            + "reserved token for implicit column."
            + " You cannot use them as a column name.");

      }
      tableSchema = tableSchema.field(
          tableElement.getName(),
          LogicalSchemas.fromSqlTypeConverter().fromSqlType(tableElement.getType())
      );
    }

    return tableSchema.build();
  }

  static void checkMetaData(
      final MetaStore metaStore,
      final String sourceName,
      final String topicName
  ) {
    // TODO: move the check to the runtime since it accesses metaStore
    if (metaStore.getSource(sourceName) != null) {
      throw new KsqlException(String.format("Source already exists: %s", sourceName));
    }

    if (metaStore.getTopic(topicName) == null) {
      throw new KsqlException(
          String.format("The corresponding topic does not exist: %s", topicName));
    }
  }

  private RegisterTopicCommand registerTopicFirst(
      final Map<String, Expression> properties
  ) {
    final Expression topicNameExp = properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY);

    if (topicNameExp == null) {
      throw new KsqlException("Corresponding Kafka topic ("
          + DdlConfig.KAFKA_TOPIC_NAME_PROPERTY + ") should be set in WITH clause.");
    }

    final String kafkaTopicName = StringUtil.cleanQuotes(topicNameExp.toString());
    if (!kafkaTopicClient.isTopicExists(kafkaTopicName)) {
      throw new KsqlException("Kafka topic does not exist: " + kafkaTopicName);
    }

    return new RegisterTopicCommand(this.topicName, false, properties);
  }


  private static void validateWithClause(final Set<String> withClauseVariables) {

    final Set<String> validSet = new HashSet<>();
    validSet.add(DdlConfig.VALUE_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.KEY_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.WINDOW_TYPE_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.TIMESTAMP_NAME_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.TOPIC_NAME_PROPERTY.toUpperCase());
    validSet.add(KsqlConstants.AVRO_SCHEMA_ID.toUpperCase());
    validSet.add(DdlConfig.TIMESTAMP_FORMAT_PROPERTY.toUpperCase());
    validSet.add(DdlConfig.VALUE_AVRO_SCHEMA_FULL_NAME.toUpperCase());

    for (final String withVariable : withClauseVariables) {
      if (!validSet.contains(withVariable.toUpperCase())) {
        throw new KsqlException("Invalid config variable in the WITH clause: " + withVariable);
      }
    }
  }

  private static SerdeFactory<?> extractKeySerde(
      final Map<String, Expression> properties
  ) {
    final String windowType = StringUtil.cleanQuotes(properties
        .getOrDefault(DdlConfig.WINDOW_TYPE_PROPERTY, new StringLiteral(""))
        .toString()
        .toUpperCase());

    if (windowType.isEmpty()) {
      return (SerdeFactory) Serdes::String;
    }

    final SerdeFactory<Windowed<String>> type = WINDOW_TYPES.get(windowType);
    if (type != null) {
      return type;
    }

    throw new KsqlException(
        DdlConfig.WINDOW_TYPE_PROPERTY + " property is not set correctly"
            + ". value: " + windowType
            + ", validValues: " + WINDOW_TYPES.keySet());
  }
}
