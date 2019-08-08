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
import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;

public class InsertValuesExecutor {

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

  public void execute(
      final ConfiguredStatement<InsertValues> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    throwIfDisabled(statement.getConfig());

    final InsertValues insertValues = statement.getStatement();
    final DataSource<?> dataSource = executionContext
        .getMetaStore()
        .getSource(insertValues.getTarget().getSuffix());

    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream/table: "
          + insertValues.getTarget().getSuffix());
    }

    if (dataSource.getKsqlTopic().getKeyFormat().isWindowed()) {
      throw new KsqlException("Cannot insert values into windowed stream/table!");
    }

    final KsqlConfig config = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides());

    try {
      final RowData row = extractRow(insertValues, dataSource);
      final byte[] key = serializeKey(row.key, dataSource, config, serviceContext);
      final byte[] value = serializeValue(row.value, dataSource, config, serviceContext);

      final String topicName = dataSource.getKafkaTopicName();

      final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
          topicName,
          null,
          row.ts,
          key,
          value
      );

      producer.sendRecord(record, serviceContext, config.getProducerClientConfigProps());
    } catch (final Exception e) {
      throw new KsqlException("Failed to insert values into stream/table: "
          + insertValues.getTarget().getSuffix(), e);
    }
  }

  private void throwIfDisabled(final KsqlConfig config) {
    final boolean isEnabled = config.getBoolean(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED);

    if (canBeDisabledByConfig && !isEnabled) {
      throw new KsqlException("The server has disabled INSERT INTO ... VALUES functionality. "
          + "To enable it, restart your KSQL-server with 'ksql.insert.into.values.enabled'=true");
    }
  }

  private RowData extractRow(
      final InsertValues insertValues,
      final DataSource<?> dataSource
  ) {
    final List<String> columns = insertValues.getColumns().isEmpty()
        ? implicitColumns(dataSource, insertValues.getValues())
        : insertValues.getColumns();

    final LogicalSchema schema = dataSource.getSchema();

    final Map<String, Object> values = resolveValues(insertValues, columns, schema);

    handleExplicitKeyField(values, dataSource.getKeyField());

    final long ts = (long) values.getOrDefault(SchemaUtil.ROWTIME_NAME, clock.getAsLong());

    final Struct key = buildKey(schema, values);
    final GenericRow value = buildValue(schema, values);

    return RowData.of(ts, key, value);
  }

  private static Struct buildKey(
      final LogicalSchema schema,
      final Map<String, Object> values
  ) {
    final Struct key = new Struct(schema.keySchema());

    for (final org.apache.kafka.connect.data.Field field : key.schema().fields()) {
      final Object value = values.get(field.name());
      key.put(field, value);
    }

    return key;
  }

  private static GenericRow buildValue(
      final LogicalSchema schema,
      final Map<String, Object> values
  ) {
    return new GenericRow(
        schema
            .valueFields()
            .stream()
            .map(Field::name)
            .map(values::get)
            .collect(Collectors.toList())
    );
  }

  @SuppressWarnings("UnstableApiUsage")
  private static List<String> implicitColumns(
      final DataSource<?> dataSource,
      final List<Expression> values
  ) {
    final LogicalSchema schema = dataSource.getSchema();

    final List<String> fieldNames = Streams.concat(
        schema.keyFields().stream(),
        schema.valueFields().stream())
        .map(Field::name)
        .collect(Collectors.toList());

    if (fieldNames.size() != values.size()) {
      throw new KsqlException(
          "Expected a value for each column."
              + " Expected Columns: " + fieldNames
              + ". Got " + values);
    }

    return fieldNames;
  }

  private static Map<String, Object> resolveValues(
      final InsertValues insertValues,
      final List<String> columns,
      final LogicalSchema schema
  ) {
    final Map<String, Object> values = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      final String column = columns.get(i);
      final SqlType columnType = columnType(column, schema);
      final Expression valueExp = insertValues.getValues().get(i);

      final Object value = new ExpressionResolver(columnType, column)
          .process(valueExp, null);

      values.put(column, value);
    }
    return values;
  }

  private static void handleExplicitKeyField(
      final Map<String, Object> values,
      final KeyField keyField
  ) {
    final Optional<String> keyFieldName = keyField.name();
    if (keyFieldName.isPresent()) {
      final String key = keyFieldName.get();
      final Object keyValue = values.get(key);
      final Object rowKeyValue = values.get(SchemaUtil.ROWKEY_NAME);

      if (keyValue != null ^ rowKeyValue != null) {
        values.putIfAbsent(key, rowKeyValue);
        values.putIfAbsent(SchemaUtil.ROWKEY_NAME, keyValue);
      } else if (!Objects.equals(keyValue, rowKeyValue)) {
        throw new KsqlException(
            String.format(
                "Expected ROWKEY and %s to match but got %s and %s respectively.",
                key, rowKeyValue, keyValue));
      }
    }
  }

  private static SqlType columnType(final String column, final LogicalSchema schema) {
    return schema.findField(column)
        .map(Field::type)
        .orElseThrow(IllegalStateException::new);
  }

  private byte[] serializeKey(
      final Struct keyValue,
      final DataSource<?> dataSource,
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
      final DataSource<?> dataSource,
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
      if (dataSource.getKsqlTopic().getValueFormat().getFormat() == Format.AVRO) {
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

      throw new KsqlException("Could not serialize row: " + row, e);
    }
  }

  @SuppressWarnings("TryFinallyCanBeTryWithResources")
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
    } catch (InterruptedException e) {
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

    private final SqlType fieldType;
    private final String fieldName;
    private final SqlValueCoercer defaultSqlValueCoercer = new DefaultSqlValueCoercer();

    ExpressionResolver(final SqlType fieldType, final String fieldName) {
      this.fieldType = Objects.requireNonNull(fieldType, "fieldType");
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    @Override
    protected String visitExpression(final Expression expression, final Void context) {
      throw new KsqlException(
          "Only Literals are supported for INSERT INTO. Got: "
              + expression + " for field " + fieldName);
    }

    @Override
    protected Object visitLiteral(final Literal node, final Void context) {
      final Object value = node.getValue();
      if (node instanceof NullLiteral || value == null) {
        return null;
      }

      return defaultSqlValueCoercer.coerce(value, fieldType)
          .orElseThrow(() -> new KsqlException(
              "Expected type " + fieldType
                  + " for field " + fieldName
                  + " but got " + value));
    }
  }
}