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
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.SerdeOptionsFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

public final class CreateSourceFactory {

  private final ServiceContext serviceContext;
  private final SerdeOptionsSupplier serdeOptionsSupplier;
  private final KeySerdeFactory keySerdeFactory;
  private final ValueSerdeFactory valueSerdeFactory;

  public CreateSourceFactory(final ServiceContext serviceContext) {
    this(
        serviceContext,
        SerdeOptionsFactory::build,
        new GenericKeySerDe(),
        new GenericRowSerDe()
    );
  }

  @VisibleForTesting
  CreateSourceFactory(
      final ServiceContext serviceContext,
      final SerdeOptionsSupplier serdeOptionsSupplier,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.serdeOptionsSupplier =
        Objects.requireNonNull(serdeOptionsSupplier, "serdeOptionsSupplier");
    this.keySerdeFactory = Objects.requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = Objects.requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

  public CreateStreamCommand createStreamCommand(
      final CreateStream statement,
      final KsqlConfig ksqlConfig
  ) {
    final SourceName sourceName = statement.getName();
    final CreateSourceProperties props = statement.getProperties();
    final String topicName = ensureTopicExists(props, serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<TimestampColumn> timestampColumn =
        buildTimestampColumn(ksqlConfig, props, schema);

    final FormatInfo keyFormat = SourcePropertiesUtil.getKeyFormat(props);
    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(props);

    final SerdeOptions serdeOptions = serdeOptionsSupplier.build(
        schema,
        FormatFactory.of(keyFormat),
        FormatFactory.of(valueFormat),
        props.getSerdeOptions(),
        ksqlConfig
    );

    validateSerdesCanHandleSchemas(ksqlConfig, schema, serdeOptions, keyFormat, valueFormat);

    return new CreateStreamCommand(
        sourceName,
        schema,
        timestampColumn,
        topicName,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        getWindowInfo(props),
        Optional.of(statement.isOrReplace())
    );
  }

  public CreateTableCommand createTableCommand(
      final CreateTable statement,
      final KsqlConfig ksqlConfig
  ) {
    final SourceName sourceName = statement.getName();
    final CreateSourceProperties props = statement.getProperties();
    final String topicName = ensureTopicExists(props, serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    if (schema.key().isEmpty()) {
      final boolean usingSchemaInference = props.getSchemaId().isPresent();

      final String additional = usingSchemaInference
          ? System.lineSeparator()
          + "Use a partial schema to define the primary key and still load the value columns from "
          + "the Schema Registry, for example:"
          + System.lineSeparator()
          + "\tCREATE TABLE " + sourceName.text() + " (ID INT PRIMARY KEY) WITH (...);"
          : "";

      throw new KsqlException(
          "Tables require a PRIMARY KEY. Please define the PRIMARY KEY." + additional
      );
    }

    final Optional<TimestampColumn> timestampColumn =
        buildTimestampColumn(ksqlConfig, props, schema);

    final FormatInfo keyFormat = SourcePropertiesUtil.getKeyFormat(props);
    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(props);

    final SerdeOptions serdeOptions = serdeOptionsSupplier.build(
        schema,
        FormatFactory.of(keyFormat),
        FormatFactory.of(valueFormat),
        props.getSerdeOptions(),
        ksqlConfig
    );

    validateSerdesCanHandleSchemas(ksqlConfig, schema, serdeOptions, keyFormat, valueFormat);

    return new CreateTableCommand(
        sourceName,
        schema,
        timestampColumn,
        topicName,
        Formats.of(keyFormat, valueFormat, serdeOptions),
        getWindowInfo(props),
        Optional.of(statement.isOrReplace())
    );
  }

  private static LogicalSchema buildSchema(final TableElements tableElements) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KsqlException("The statement does not define any columns.");
    }

    tableElements.forEach(e -> {
      if (SystemColumns.isSystemColumn(e.getName())) {
        throw new KsqlException("'" + e.getName().text() + "' is a reserved column name.");
      }
    });

    return tableElements.toLogicalSchema();
  }

  private static Optional<WindowInfo> getWindowInfo(final CreateSourceProperties props) {
    return props.getWindowType().map(type -> WindowInfo.of(type, props.getWindowSize()));
  }

  private static String ensureTopicExists(
      final CreateSourceProperties properties,
      final ServiceContext serviceContext
  ) {
    final String kafkaTopicName = properties.getKafkaTopic();
    if (!serviceContext.getTopicClient().isTopicExists(kafkaTopicName)) {
      throw new KsqlException("Kafka topic does not exist: " + kafkaTopicName);
    }

    return kafkaTopicName;
  }

  private static Optional<TimestampColumn> buildTimestampColumn(
      final KsqlConfig ksqlConfig,
      final CreateSourceProperties properties,
      final LogicalSchema schema
  ) {
    final Optional<ColumnName> timestampName = properties.getTimestampColumnName();
    final Optional<TimestampColumn> timestampColumn = timestampName.map(
        n -> new TimestampColumn(n, properties.getTimestampFormat())
    );
    // create the final extraction policy to validate that the ref/format are OK
    TimestampExtractionPolicyFactory.validateTimestampColumn(ksqlConfig, schema, timestampColumn);
    return timestampColumn;
  }

  private void validateSerdesCanHandleSchemas(
      final KsqlConfig ksqlConfig,
      final LogicalSchema schema,
      final SerdeOptions serdeOptions,
      final FormatInfo keyFormat,
      final FormatInfo valueFormat
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(schema, serdeOptions);

    keySerdeFactory.create(
        keyFormat,
        physicalSchema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    ).close();

    valueSerdeFactory.create(
        valueFormat,
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    ).close();
  }

  @FunctionalInterface
  interface SerdeOptionsSupplier {

    SerdeOptions build(
        LogicalSchema schema,
        Format keyFormat,
        Format valueFormat,
        SerdeOptions explicitOptions,
        KsqlConfig ksqlConfig
    );
  }
}
