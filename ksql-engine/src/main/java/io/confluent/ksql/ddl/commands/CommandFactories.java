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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;

import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.ddl.commands.CreateStreamCommand;
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.DropType;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
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
import io.confluent.ksql.util.HandlerMaps;
import io.confluent.ksql.util.HandlerMaps.ClassHandlerMapR2;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicyFactory;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandFactories implements DdlCommandFactory {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ClassHandlerMapR2<DdlStatement, CommandFactories, CallInfo, DdlCommand>
      FACTORIES = HandlerMaps
      .forClass(DdlStatement.class)
      .withArgTypes(CommandFactories.class, CallInfo.class)
      .withReturnType(DdlCommand.class)
      .put(CreateStream.class, CommandFactories::handleCreateStream)
      .put(CreateTable.class, CommandFactories::handleCreateTable)
      .put(DropStream.class, CommandFactories::handleDropStream)
      .put(DropTable.class, CommandFactories::handleDropTable)
      .put(RegisterType.class, CommandFactories::handleRegisterType)
      .put(DropType.class, CommandFactories::handleDropType)
      .build();

  private final ServiceContext serviceContext;
  private final MetaStore metaStore;
  private final SerdeOptionsSupplier serdeOptionsSupplier;
  private final ValueSerdeFactory serdeFactory;

  public CommandFactories(final ServiceContext serviceContext, final MetaStore metaStore) {
    this(serviceContext, metaStore, SerdeOptions::buildForCreateStatement, new GenericRowSerDe());
  }

  public CommandFactories(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final SerdeOptionsSupplier serdeOptionsSupplier,
      final ValueSerdeFactory serdeFactory) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
    this.serdeOptionsSupplier =
        Objects.requireNonNull(serdeOptionsSupplier, "serdeOptionsSupplier");
    this.serdeFactory = Objects.requireNonNull(serdeFactory, "serdeFactory");
  }

  @Override
  public DdlCommand create(
      final String sqlExpression,
      final DdlStatement ddlStatement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> properties
  ) {
    return FACTORIES
        .getOrDefault(ddlStatement.getClass(), (statement, cf, ci) -> {
          throw new KsqlException(
              "Unable to find ddl command factory for statement:"
                  + statement.getClass()
                  + " valid statements:"
                  + FACTORIES.keySet()
          );
        })
        .handle(
            this,
            new CallInfo(sqlExpression, ksqlConfig, properties),
            ddlStatement);
  }

  private CreateStreamCommand handleCreateStream(
      final CallInfo callInfo,
      final CreateStream statement
  ) {
    final String sourceName = statement.getName().name();
    final KsqlTopic topic = buildTopic(statement.getProperties(), serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<String> keyFieldName = buildKeyFieldName(statement, schema);
    final TimestampExtractionPolicy timestampExtractionPolicy = buildTimestampExtractor(
        callInfo.ksqlConfig,
        statement.getProperties(),
        schema
    );
    final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        callInfo.ksqlConfig
    );
    validateSerdeCanHandleSchemas(
        callInfo.ksqlConfig,
        serviceContext,
        serdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
    return new CreateStreamCommand(
        callInfo.sqlExpression,
        sourceName,
        schema,
        keyFieldName,
        timestampExtractionPolicy,
        serdeOptions,
        topic
    );
  }

  private CreateTableCommand handleCreateTable(
      final CallInfo callInfo,
      final CreateTable statement
  ) {
    final String sourceName = statement.getName().name();
    final KsqlTopic topic = buildTopic(statement.getProperties(), serviceContext);
    final LogicalSchema schema = buildSchema(statement.getElements());
    final Optional<String> keyFieldName = buildKeyFieldName(statement, schema);
    final TimestampExtractionPolicy timestampExtractionPolicy = buildTimestampExtractor(
        callInfo.ksqlConfig,
        statement.getProperties(),
        schema
    );
    final Set<SerdeOption> serdeOptions = serdeOptionsSupplier.build(
        schema,
        topic.getValueFormat().getFormat(),
        statement.getProperties().getWrapSingleValues(),
        callInfo.ksqlConfig
    );
    validateSerdeCanHandleSchemas(
        callInfo.ksqlConfig,
        serviceContext,
        serdeFactory,
        PhysicalSchema.from(schema, serdeOptions),
        topic
    );
    return new CreateTableCommand(
        callInfo.sqlExpression,
        sourceName,
        schema,
        keyFieldName,
        timestampExtractionPolicy,
        serdeOptions,
        topic
    );
  }

  @SuppressWarnings("MethodMayBeStatic")
  private DropSourceCommand handleDropStream(final DropStream statement) {
    return handleDropSource(
        statement.getName().name(),
        statement.getIfExists(),
        DataSourceType.KSTREAM
    );
  }

  @SuppressWarnings("MethodMayBeStatic")
  private DropSourceCommand handleDropTable(final DropTable statement) {
    return handleDropSource(
        statement.getName().name(),
        statement.getIfExists(),
        DataSourceType.KTABLE
    );
  }

  @SuppressWarnings("MethodMayBeStatic")
  private RegisterTypeCommand handleRegisterType(final RegisterType statement) {
    return new RegisterTypeCommand(
        statement.getType().getSqlType(),
        statement.getName()
    );
  }

  private DropTypeCommand handleDropType(final DropType statement) {
    return new DropTypeCommand(statement.getTypeName());
  }

  private DropSourceCommand handleDropSource(
      final String sourceName,
      final boolean ifExists,
      final DataSourceType dataSourceType) {
    final DataSource<?> dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      if (ifExists) {
        throw new KsqlException("Source " + sourceName + " does not exist.");
      }
    } else if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KsqlException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType() == DataSourceType.KSTREAM ? "STREAM" : "TABLE",
          dataSourceType == DataSourceType.KSTREAM ? "STREAM" : "TABLE"
      ));
    }
    return new DropSourceCommand(sourceName);
  }

  private static Optional<String> buildKeyFieldName(
      final CreateSource statement,
      final LogicalSchema schema) {
    if (statement.getProperties().getKeyField().isPresent()) {
      final String name = statement.getProperties().getKeyField().get().toUpperCase();
      final String cleanName = StringUtil.cleanQuotes(name);
      schema.findValueColumn(cleanName).orElseThrow(
          () -> new KsqlException(
              "The KEY column set in the WITH clause does not exist in the schema: '"
                  + cleanName + "'"
          )
      );
      return Optional.of(cleanName);
    } else {
      return Optional.empty();
    }
  }

  private static LogicalSchema buildSchema(final TableElements tableElements) {
    if (Iterables.isEmpty(tableElements)) {
      throw new KsqlException("The statement does not define any columns.");
    }

    tableElements.forEach(e -> {
      if (e.getName().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)) {
        throw new KsqlException("'" + e.getName() + "' is a reserved field name.");
      }

      final boolean isRowKey = e.getName().toUpperCase().equals(SchemaUtil.ROWKEY_NAME);

      if (e.getNamespace() == Namespace.KEY) {
        if (!isRowKey) {
          throw new KsqlException("'" + e.getName() + "' is an invalid KEY field name. "
              + "KSQL currently only supports KEY fields named ROWKEY.");
        }

        if (e.getType().getSqlType().baseType() != SqlBaseType.STRING) {
          throw new KsqlException("'" + e.getName() + "' is a KEY field with an unsupported type. "
              + "KSQL currently only supports KEY fields of type " + SqlBaseType.STRING + ".");
        }
      } else if (isRowKey) {
        throw new KsqlException("'" + e.getName() + "' is a reserved field name. "
            + "It can only be used for KEY fields.");
      }
    });

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

  private static final class CallInfo {

    final String sqlExpression;
    final KsqlConfig ksqlConfig;
    final Map<String, Object> properties;

    private CallInfo(
        final String sqlExpression,
        final KsqlConfig ksqlConfig,
        final Map<String, Object> properties
    ) {
      this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
      this.properties = Objects.requireNonNull(properties, "properties");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig")
          .cloneWithPropertyOverwrite(properties);
    }
  }
}
