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

package io.confluent.ksql.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class InsertValuesExecutor {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger LOG = LoggerFactory.getLogger(InsertValuesExecutor.class);
  private static final Duration MAX_SEND_TIMEOUT = Duration.ofSeconds(5);

  private final LongSupplier clock;
  private final boolean canBeDisabledByConfig;
  private final RecordProducer producer;
  private final ValueSerdeFactory valueSerdeFactory;
  private final KeySerdeFactory keySerdeFactory;

  public InsertValuesExecutor() {
    this(true, InsertValuesExecutor::sendRecord);
  }

  public interface RecordProducer {

    void sendRecord(
            ProducerRecord<byte[], byte[]> record,
            ServiceContext serviceContext,
            Map<String, Object> producerProps
    );
  }

  @VisibleForTesting
  InsertValuesExecutor(
          final boolean canBeDisabledByConfig,
          final RecordProducer producer
  ) {
    this(
            producer,
            canBeDisabledByConfig,
            System::currentTimeMillis,
            new GenericKeySerDe(),
            new GenericRowSerDe()
    );
  }

  @VisibleForTesting
  InsertValuesExecutor(
          final LongSupplier clock,
          final KeySerdeFactory keySerdeFactory,
          final ValueSerdeFactory valueSerdeFactory
  ) {
    this(InsertValuesExecutor::sendRecord, true, clock, keySerdeFactory, valueSerdeFactory);
  }

  private InsertValuesExecutor(
          final RecordProducer producer,
          final boolean canBeDisabledByConfig,
          final LongSupplier clock,
          final KeySerdeFactory keySerdeFactory,
          final ValueSerdeFactory valueSerdeFactory
  ) {
    this.canBeDisabledByConfig = canBeDisabledByConfig;
    this.producer = Objects.requireNonNull(producer, "producer");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.keySerdeFactory = Objects.requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.valueSerdeFactory = Objects.requireNonNull(valueSerdeFactory, "valueSerdeFactory");
  }

  @SuppressWarnings("unused") // Part of required API.
  public void execute(
          final ConfiguredStatement<InsertValues> statement,
          final SessionProperties sessionProperties,
          final KsqlExecutionContext executionContext,
          final ServiceContext serviceContext
  ) {
    final InsertValues insertValues = statement.getStatement();
    final MetaStore metaStore = executionContext.getMetaStore();
    final KsqlConfig config = statement.getConfig()
            .cloneWithPropertyOverwrite(statement.getConfigOverrides());

    final DataSource dataSource = getDataSource(config, metaStore, insertValues);

    final ProducerRecord<byte[], byte[]> record =
            buildRecord(statement, metaStore, dataSource, serviceContext);

    try {
      producer.sendRecord(record, serviceContext, config.getProducerClientConfigProps());
    } catch (final TopicAuthorizationException e) {
      // TopicAuthorizationException does not give much detailed information about why it failed,
      // except which topics are denied. Here we just add the ACL to make the error message
      // consistent with other authorization error messages.
      final Exception rootCause = new KsqlTopicAuthorizationException(
              AclOperation.WRITE,
              e.unauthorizedTopics()
      );

      throw new KsqlException(createInsertFailedExceptionMessage(insertValues), rootCause);
    } catch (final Exception e) {
      throw new KsqlException(createInsertFailedExceptionMessage(insertValues), e);
    }
  }

  private static DataSource getDataSource(
          final KsqlConfig ksqlConfig,
          final MetaStore metaStore,
          final InsertValues insertValues
  ) {
    final DataSource dataSource = metaStore.getSource(insertValues.getTarget());
    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream/table: "
              + insertValues.getTarget());
    }

    if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
      throw new KsqlException("Cannot insert values into windowed stream/table!");
    }

    final ReservedInternalTopics internalTopics = new ReservedInternalTopics(ksqlConfig);
    if (internalTopics.isReadOnly(dataSource.getKafkaTopicName())) {
      throw new KsqlException("Cannot insert values into read-only topic: "
              + dataSource.getKafkaTopicName());
    }

    return dataSource;
  }

  private ProducerRecord<byte[], byte[]> buildRecord(
          final ConfiguredStatement<InsertValues> statement,
          final MetaStore metaStore,
          final DataSource dataSource,
          final ServiceContext serviceContext
  ) {
    throwIfDisabled(statement.getConfig());

    final InsertValues insertValues = statement.getStatement();
    final KsqlConfig config = statement.getConfig()
            .cloneWithPropertyOverwrite(statement.getConfigOverrides());

    try {
      final RowData row = extractRow(
              insertValues,
              dataSource,
              metaStore,
              config);

      final byte[] key = serializeKey(row.key, dataSource, config, serviceContext);
      final byte[] value = serializeValue(row.value, dataSource, config, serviceContext);

      final String topicName = dataSource.getKafkaTopicName();

      return new ProducerRecord<>(
              topicName,
              null,
              row.ts,
              key,
              value
      );
    } catch (final Exception e) {
      throw new KsqlStatementException(
              createInsertFailedExceptionMessage(insertValues) + " " + e.getMessage(),
              statement.getStatementText(),
              e);
    }
  }

  private static String createInsertFailedExceptionMessage(final InsertValues insertValues) {
    return "Failed to insert values into '" + insertValues.getTarget().text() + "'.";
  }

  private void throwIfDisabled(final KsqlConfig config) {
    final boolean isEnabled = config.getBoolean(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED);

    if (canBeDisabledByConfig && !isEnabled) {
      throw new KsqlException("The server has disabled INSERT INTO ... VALUES functionality. "
              + "To enable it, restart your KSQL-server "
              + "with 'ksql.insert.into.values.enabled'=true");
    }
  }

  private RowData extractRow(
          final InsertValues insertValues,
          final DataSource dataSource,
          final FunctionRegistry functionRegistry,
          final KsqlConfig config
  ) {
    final List<ColumnName> columns = insertValues.getColumns().isEmpty()
            ? implicitColumns(dataSource, insertValues.getValues())
            : insertValues.getColumns();

    final LogicalSchema schemaWithRowTime = withRowTime(dataSource.getSchema());

    for (ColumnName col : columns) {
      if (!schemaWithRowTime.findColumn(col).isPresent()) {
        throw new KsqlException("Column name " + col + " does not exist.");
      }
    }

    final Map<ColumnName, Object> values = resolveValues(
            insertValues, columns, schemaWithRowTime, functionRegistry, config);

    if (dataSource.getDataSourceType() == DataSourceType.KTABLE) {
      final String noValue = schemaWithRowTime.key().stream()
              .map(Column::name)
              .filter(colName -> !values.containsKey(colName))
              .map(ColumnName::text)
              .collect(Collectors.joining(", "));

      if (!noValue.isEmpty()) {
        throw new KsqlException("Value for primary key column(s) "
                + noValue + " is required for tables");
      }
    }

    final long ts = (long) values.getOrDefault(SystemColumns.ROWTIME_NAME, clock.getAsLong());

    final Struct key = buildKey(dataSource.getSchema(), values);
    final GenericRow value = buildValue(dataSource.getSchema(), values);

    return RowData.of(ts, key, value);
  }

  private static LogicalSchema withRowTime(final LogicalSchema schema) {
    // The set of columns users can supply values for includes the ROWTIME pseudocolumn,
    // so include it in the schema:
    return schema.asBuilder()
            .valueColumn(SystemColumns.ROWTIME_NAME, SystemColumns.ROWTIME_TYPE)
            .build();
  }

  private static Struct buildKey(
          final LogicalSchema schema,
          final Map<ColumnName, Object> values
  ) {

    final Struct key = new Struct(schema.keyConnectSchema());

    for (final org.apache.kafka.connect.data.Field field : key.schema().fields()) {
      final Object value = values.get(ColumnName.of(field.name()));
      key.put(field, value);
    }

    return key;
  }

  private static GenericRow buildValue(
          final LogicalSchema schema,
          final Map<ColumnName, Object> values
  ) {
    return new GenericRow().appendAll(
            schema
                    .value()
                    .stream()
                    .map(Column::name)
                    .map(values::get)
                    .collect(Collectors.toList())
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  private static List<ColumnName> implicitColumns(
          final DataSource dataSource,
          final List<Expression> values
  ) {
    final LogicalSchema schema = dataSource.getSchema();

    final List<ColumnName> fieldNames = Streams.concat(
            schema.key().stream(),
            schema.value().stream())
            .map(Column::name)
            .collect(Collectors.toList());

    if (fieldNames.size() != values.size()) {
      throw new KsqlException(
              "Expected a value for each column."
                      + " Expected Columns: " + fieldNames
                      + ". Got " + values);
    }

    return fieldNames;
  }

  private static Map<ColumnName, Object> resolveValues(
          final InsertValues insertValues,
          final List<ColumnName> columns,
          final LogicalSchema schema,
          final FunctionRegistry functionRegistry,
          final KsqlConfig config
  ) {
    final Map<ColumnName, Object> values = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      final ColumnName column = columns.get(i);
      final SqlType columnType = columnType(column, schema);
      final Expression valueExp = insertValues.getValues().get(i);

      final Object value =
              new ExpressionResolver(columnType, column, schema, functionRegistry, config)
                      .process(valueExp, null);

      values.put(column, value);
    }
    return values;
  }

  private static SqlType columnType(final ColumnName column, final LogicalSchema schema) {
    return schema
            .findColumn(column)
            .map(Column::type)
            .orElseThrow(IllegalStateException::new);
  }

  private byte[] serializeKey(
          final Struct keyValue,
          final DataSource dataSource,
          final KsqlConfig config,
          final ServiceContext serviceContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
            dataSource.getSchema(),
            dataSource.getSerdeOptions()
    );

    final Serde<Struct> keySerde = keySerdeFactory.create(
        dataSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
        physicalSchema.keySchema(),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );

    try {
      return keySerde
              .serializer()
              .serialize(dataSource.getKafkaTopicName(), keyValue);
    } catch (final Exception e) {
      throw new KsqlException("Could not serialize key: " + keyValue, e);
    }
  }

  private byte[] serializeValue(
          final GenericRow row,
          final DataSource dataSource,
          final KsqlConfig config,
          final ServiceContext serviceContext
  ) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
            dataSource.getSchema(),
            dataSource.getSerdeOptions()
    );

    final Serde<GenericRow> valueSerde = valueSerdeFactory.create(
            dataSource.getKsqlTopic().getValueFormat().getFormatInfo(),
            physicalSchema.valueSchema(),
            config,
            serviceContext.getSchemaRegistryClientFactory(),
            "",
            NoopProcessingLogContext.INSTANCE
    );

    final String topicName = dataSource.getKafkaTopicName();

    try {
      return valueSerde.serializer().serialize(topicName, row);
    } catch (final Exception e) {
      if (dataSource.getKsqlTopic().getValueFormat().getFormat().supportsSchemaInference()) {
        final Throwable rootCause = ExceptionUtils.getRootCause(e);
        if (rootCause instanceof RestClientException) {
          switch (((RestClientException) rootCause).getStatus()) {
            case HttpStatus.SC_UNAUTHORIZED:
            case HttpStatus.SC_FORBIDDEN:
              throw new KsqlException(String.format(
                      "Not authorized to write Schema Registry subject: [%s]",
                      topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX
              ));
            default:
              break;
          }
        }
      }

      LOG.error("Could not serialize row.", e);
      throw new KsqlException("Could not serialize row: " + row + ". " + e.getMessage(), e);
    }
  }

  private static void sendRecord(
      final ProducerRecord<byte[], byte[]> record,
      final ServiceContext serviceContext,
      final Map<String, Object> producerProps
  ) {
    // for now, just create a new producer each time
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(producerProps);

    final Future<RecordMetadata> producerCallResult;

    try {
      producerCallResult = producer.send(record);
    } finally {
      producer.close(MAX_SEND_TIMEOUT);
    }

    try {
      // Check if the producer failed to write to the topic. This can happen if the
      // ServiceContext does not have write permissions.
      producerCallResult.get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static final class RowData {

    final long ts;
    final Struct key;
    final GenericRow value;

    private static RowData of(final long ts, final Struct key, final GenericRow value) {
      return new RowData(ts, key, value);
    }

    private RowData(final long ts, final Struct key, final GenericRow value) {
      this.ts = ts;
      this.key = key;
      this.value = value;
    }
  }

  private static class ExpressionResolver extends VisitParentExpressionVisitor<Object, Void> {

    private static final Supplier<String> IGNORED_MSG = () -> "";
    private static final ProcessingLogger THROWING_LOGGER = errorMessage -> {
      throw new KsqlException(((RecordProcessingError) errorMessage).getMessage());
    };

    private final SqlType fieldType;
    private final ColumnName fieldName;
    private final LogicalSchema schema;
    private final SqlValueCoercer sqlValueCoercer = DefaultSqlValueCoercer.INSTANCE;
    private final FunctionRegistry functionRegistry;
    private final KsqlConfig config;

    ExpressionResolver(
        final SqlType fieldType,
        final ColumnName fieldName,
        final LogicalSchema schema,
        final FunctionRegistry functionRegistry,
        final KsqlConfig config
    ) {
      this.fieldType = Objects.requireNonNull(fieldType, "fieldType");
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.schema = Objects.requireNonNull(schema, "schema");
      this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
      this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    protected Object visitExpression(final Expression expression, final Void context) {
      final ExpressionMetadata metadata =
          Iterables.getOnlyElement(
              CodeGenRunner.compileExpressions(
                  Stream.of(expression),
                  "insert value",
                  schema,
                  config,
                  functionRegistry)
          );

      // we expect no column references, so we can pass in an empty generic row
      final Object value = metadata.evaluate(new GenericRow(), null, THROWING_LOGGER, IGNORED_MSG);

      return sqlValueCoercer.coerce(value, fieldType)
          .orElseThrow(() -> {
            final SqlBaseType valueSqlType = SchemaConverters.javaToSqlConverter()
                .toSqlType(value.getClass());

            return new KsqlException(
                String.format("Expected type %s for field %s but got %s(%s)",
                    fieldType,
                    fieldName,
                    valueSqlType,
                    value));
          })
          .orElse(null);
    }

    @Override
    public Object visitNullLiteral(final NullLiteral node, final Void context) {
      return null;
    }
  }
}