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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.streams.timestamp.TimestampExtractionPolicyFactory;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;

public final class CreateSourceFactory {

  private final ServiceContext serviceContext;
  private final SerdeFeaturessSupplier keySerdeFeaturesSupplier;
  private final SerdeFeaturessSupplier valueSerdeFeaturesSupplier;
  private final KeySerdeFactory keySerdeFactory;
  private final ValueSerdeFactory valueSerdeFactory;
  private final MetaStore metaStore;

  public CreateSourceFactory(final ServiceContext serviceContext, final MetaStore metaStore) {
    this(
        serviceContext,
        (s, f, e, k) -> SerdeFeaturesFactory.buildKeyFeatures(s, f),
        SerdeFeaturesFactory::buildValueFeatures,
        new GenericKeySerDe(),
        new GenericRowSerDe(),
        metaStore
    );
  }

  @VisibleForTesting
  CreateSourceFactory(
      final ServiceContext serviceContext,
      final SerdeFeaturessSupplier keySerdeFeaturesSupplier,
      final SerdeFeaturessSupplier valueSerdeFeaturesSupplier,
      final KeySerdeFactory keySerdeFactory,
      final ValueSerdeFactory valueSerdeFactory,
      final MetaStore metaStore
  ) {
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.keySerdeFeaturesSupplier =
        requireNonNull(keySerdeFeaturesSupplier, "keySerdeFeaturesSupplier");
    this.valueSerdeFeaturesSupplier =
        requireNonNull(valueSerdeFeaturesSupplier, "valueSerdeFeaturesSupplier");
    this.keySerdeFactory = requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = requireNonNull(valueSerdeFactory, "valueSerdeFactory");
    this.metaStore = requireNonNull(metaStore);
  }

  // This method is called by CREATE_AS statements
  public CreateStreamCommand createStreamCommand(final KsqlStructuredDataOutputNode outputNode) {
    return new CreateStreamCommand(
        outputNode.getSinkName().get(),
        outputNode.getSchema(),
        outputNode.getTimestampColumn(),
        outputNode.getKsqlTopic().getKafkaTopicName(),
        Formats.from(outputNode.getKsqlTopic()),
        outputNode.getKsqlTopic().getKeyFormat().getWindowInfo(),
        Optional.of(outputNode.getOrReplace()),
        Optional.of(false)
    );
  }

  // This method is called by simple CREATE statements
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
    final DataSource dataSource = metaStore.getSource(sourceName);

    if (dataSource != null && !statement.isOrReplace() && !statement.isNotExists()) {
      final String sourceType = dataSource.getDataSourceType().getKsqlType();
      throw new KsqlException(
          String.format("Cannot add stream '%s': A %s with the same name already exists",
             sourceName.text(), sourceType.toLowerCase()));
    }

    throwIfCreateOrReplaceOnSourceStreamOrTable(statement, dataSource);

    return new CreateStreamCommand(
        sourceName,
        schema,
        timestampColumn,
        topicName,
        buildFormats(statement.getName(), schema, props, ksqlConfig),
        getWindowInfo(props),
        Optional.of(statement.isOrReplace()),
        Optional.of(statement.isSource())
    );
  }

  // This method is called by CREATE_AS statements
  public CreateTableCommand createTableCommand(
      final KsqlStructuredDataOutputNode outputNode,
      final Optional<RefinementInfo> emitStrategy
  ) {
    Optional<WindowInfo> windowInfo =
        outputNode.getKsqlTopic().getKeyFormat().getWindowInfo();

    if (windowInfo.isPresent() && emitStrategy.isPresent()) {
      final WindowInfo info = windowInfo.get();
      windowInfo = Optional.of(WindowInfo.of(
          info.getType(),
          info.getSize(),
          Optional.of(emitStrategy.get().getOutputRefinement())
      ));
    }

    return new CreateTableCommand(
        outputNode.getSinkName().get(),
        outputNode.getSchema(),
        outputNode.getTimestampColumn(),
        outputNode.getKsqlTopic().getKafkaTopicName(),
        Formats.from(outputNode.getKsqlTopic()),
        windowInfo,
        Optional.of(outputNode.getOrReplace()),
        Optional.of(false)
    );
  }

  // This method is called by simple CREATE statements
  public CreateTableCommand createTableCommand(
      final CreateTable statement,
      final KsqlConfig ksqlConfig
  ) {
    final SourceName sourceName = statement.getName();
    final CreateSourceProperties props = statement.getProperties();
    final String topicName = ensureTopicExists(props, serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final DataSource dataSource = metaStore.getSource(sourceName);

    if (dataSource != null && !statement.isOrReplace() && !statement.isNotExists()) {
      final String sourceType = dataSource.getDataSourceType().getKsqlType();
      throw new KsqlException(
          String.format("Cannot add table '%s': A %s with the same name already exists",
              sourceName.text(), sourceType.toLowerCase()));
    }
    if (schema.key().isEmpty()) {
      final boolean usingSchemaInference = props.getValueSchemaId().isPresent();

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

    throwIfCreateOrReplaceOnSourceStreamOrTable(statement, dataSource);

    final Optional<TimestampColumn> timestampColumn =
        buildTimestampColumn(ksqlConfig, props, schema);

    return new CreateTableCommand(
        sourceName,
        schema,
        timestampColumn,
        topicName,
        buildFormats(statement.getName(), schema, props, ksqlConfig),
        getWindowInfo(props),
        Optional.of(statement.isOrReplace()),
        Optional.of(statement.isSource())
    );
  }

  private void throwIfCreateOrReplaceOnSourceStreamOrTable(
      final CreateSource createSource,
      final DataSource existingSource
  ) {
    if (createSource.isOrReplace()) {
      final String createSourceType = (createSource instanceof CreateStream) ? "stream" : "table";

      if (createSource.isSource() || (existingSource != null && existingSource.isSource())) {
        throw new KsqlException(
            String.format("Cannot add %s '%s': CREATE OR REPLACE is not supported on source %s.",
                createSourceType,
                createSource.getName().text(),
                createSourceType + "s"));
      }
    }
  }

  private Formats buildFormats(
      final SourceName name,
      final LogicalSchema schema,
      final CreateSourceProperties props,
      final KsqlConfig ksqlConfig
  ) {
    final FormatInfo keyFormat = SourcePropertiesUtil.getKeyFormat(props, name);
    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(props);

    final SerdeFeatures keyFeatures = keySerdeFeaturesSupplier.build(
        schema,
        FormatFactory.of(keyFormat),
        SerdeFeatures.of(),
        ksqlConfig
    );

    final SerdeFeatures valFeatures = valueSerdeFeaturesSupplier.build(
        schema,
        FormatFactory.of(valueFormat),
        props.getValueSerdeFeatures(),
        ksqlConfig
    );

    final Formats formats = Formats.of(keyFormat, valueFormat, keyFeatures, valFeatures);
    validateSerdesCanHandleSchemas(ksqlConfig, schema, formats);
    return formats;
  }

  private static LogicalSchema buildSchema(
      final TableElements tableElements
  ) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KsqlException("The statement does not define any columns.");
    }

    tableElements.forEach(e -> {
      if (SystemColumns.isSystemColumn(e.getName())) {
        throw new KsqlException("'" + e.getName().text() + "' is a reserved column name. "
            + "You cannot use it as a name for a column.");
      }
    });

    return tableElements.toLogicalSchema();
  }

  private static Optional<WindowInfo> getWindowInfo(final CreateSourceProperties props) {
    return props.getWindowType()
        .map(type -> WindowInfo.of(type, props.getWindowSize(), Optional.empty()));
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
      final Formats formats
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema
        .from(schema, formats.getKeyFeatures(), formats.getValueFeatures());

    keySerdeFactory.create(
        formats.getKeyFormat(),
        physicalSchema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    ).close();

    valueSerdeFactory.create(
        formats.getValueFormat(),
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    ).close();
  }

  @FunctionalInterface
  interface SerdeFeaturessSupplier {

    SerdeFeatures build(
        LogicalSchema schema,
        Format format,
        SerdeFeatures explicitFeatures,
        KsqlConfig ksqlConfig
    );
  }
}
