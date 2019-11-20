/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.TopicFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class CreateSourceFactory {
  private final ServiceContext serviceContext;
  private final SerdeOptionsSupplier serdeOptionsSupplier;
  private final ValueSerdeFactory serdeFactory;

  public CreateSourceFactory(final ServiceContext serviceContext) {
    this(serviceContext, SerdeOptions::buildForCreateStatement, new GenericRowSerDe());
  }

  @VisibleForTesting
  CreateSourceFactory(
      final ServiceContext serviceContext,
      final SerdeOptionsSupplier serdeOptionsSupplier,
      final ValueSerdeFactory serdeFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.serdeOptionsSupplier =
        Objects.requireNonNull(serdeOptionsSupplier, "serdeOptionsSupplier");
    this.serdeFactory = Objects.requireNonNull(serdeFactory, "serdeFactory");
  }

  public CreateStreamCommand createStreamCommand(
      final CreateStream statement,
      final KsqlConfig ksqlConfig
  ) {
    final SourceName sourceName = statement.getName();
    final KsqlTopic topic = buildTopic(statement.getProperties(), serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<ColumnName> keyFieldName = buildKeyFieldName(statement, schema);
    final TimestampExtractionPolicy timestampExtractionPolicy = buildTimestampExtractor(
        ksqlConfig,
        statement.getProperties(),
        schema
    );
    final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        ksqlConfig
    );
    validateSerdeCanHandleSchemas(
        ksqlConfig,
        serviceContext,
        serdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
    return new CreateStreamCommand(
        sourceName,
        schema,
        keyFieldName,
        timestampExtractionPolicy,
        serdeOptions,
        topic
    );
  }

  public CreateTableCommand createTableCommand(
      final CreateTable statement,
      final KsqlConfig ksqlConfig
  ) {
    final SourceName sourceName = statement.getName();
    final KsqlTopic topic = buildTopic(statement.getProperties(), serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<ColumnName> keyFieldName = buildKeyFieldName(statement, schema);
    final TimestampExtractionPolicy timestampExtractionPolicy = buildTimestampExtractor(
        ksqlConfig,
        statement.getProperties(),
        schema
    );
    final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        ksqlConfig
    );
    validateSerdeCanHandleSchemas(
        ksqlConfig,
        serviceContext,
        serdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
    return new CreateTableCommand(
        sourceName,
        schema,
        keyFieldName,
        timestampExtractionPolicy,
        serdeOptions,
        topic
    );
  }

  private static Optional<ColumnName> buildKeyFieldName(
      final CreateSource statement,
      final LogicalSchema schema) {
    if (statement.getProperties().getKeyField().isPresent()) {
      final ColumnRef column = statement.getProperties().getKeyField().get();
      schema.findValueColumn(column)
          .orElseThrow(() -> new KsqlException(
              "The KEY column set in the WITH clause does not exist in the schema: '"
                  + column.toString(FormatOptions.noEscape()) + "'"
          ));
      return Optional.of(column.name());
    } else {
      return Optional.empty();
    }
  }

  private static LogicalSchema buildSchema(final TableElements tableElements) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KsqlException("The statement does not define any columns.");
    }

    tableElements.forEach(e -> {
      if (e.getName().equals(SchemaUtil.ROWTIME_NAME)) {
        throw new KsqlException("'" + e.getName().name() + "' is a reserved column name.");
      }

      final boolean isRowKey = e.getName().equals(SchemaUtil.ROWKEY_NAME);

      if (e.getNamespace() == Namespace.KEY) {
        if (!isRowKey) {
          throw new KsqlException("'" + e.getName().name() + "' is an invalid KEY column name. "
              + "KSQL currently only supports KEY columns named ROWKEY.");
        }

        if (e.getType().getSqlType().baseType() != SqlBaseType.STRING) {
          throw new KsqlException("'" + e.getName().name()
              + "' is a KEY column with an unsupported type. "
              + "KSQL currently only supports KEY columns of type " + SqlBaseType.STRING + ".");
        }
      } else if (isRowKey) {
        throw new KsqlException("'" + e.getName().name() + "' is a reserved column name. "
            + "It can only be used for KEY columns.");
      }
    });

    return tableElements.toLogicalSchema(true);
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
    final Optional<ColumnRef> timestampName = properties.getTimestampColumnName();
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
