/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class InsertValuesExecutor {

  private static final long MAX_SEND_TIMEOUT_SECONDS = 5;

  private final LongSupplier clock;

  public InsertValuesExecutor() {
    this(System::currentTimeMillis);
  }

  @VisibleForTesting
  InsertValuesExecutor(final LongSupplier clock) {
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public Optional<KsqlEntity> execute(
      final ConfiguredStatement<InsertValues> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    if (!statement.getConfig().getBoolean(KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED)) {
      throw new KsqlException("The server has disabled INSERT INTO ... VALUES functionality. "
          + "To enable it, restart your KSQL-server with 'ksql.insert.into.values.enabled'=true");
    }

    final InsertValues insertValues = statement.getStatement();
    final DataSource<?> dataSource = executionContext
        .getMetaStore()
        .getSource(insertValues.getTarget().getSuffix());

    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream/table: "
          + insertValues.getTarget().getSuffix());
    }

    if (dataSource instanceof KsqlTable && ((KsqlTable<?>) dataSource).isWindowed()
        || dataSource instanceof KsqlStream && ((KsqlStream<?>) dataSource).hasWindowedKey()) {
      throw new KsqlException("Cannot insert values into windowed stream/table!");
    }

    final KsqlConfig config = statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides());

    final RowData row = extractRow(insertValues, dataSource);
    final byte[] key = serializeKey(row.key, dataSource);
    final byte[] value = serializeRow(row.value, dataSource, config, serviceContext);

    final String topicName = dataSource.getKafkaTopicName();

    // for now, just create a new producer each time
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(config.getProducerClientConfigProps());

    final Future<RecordMetadata> producerCallResult = producer.send(
        new ProducerRecord<>(
            topicName,
            null,
            row.ts,
            key,
            value
        )
    );

    producer.close(Duration.ofSeconds(MAX_SEND_TIMEOUT_SECONDS));

    try {
      // Check if the producer failed to write to the topic. This can happen if the
      // ServiceContext does not have write permissions.
      producerCallResult.get();
    } catch (final Exception e) {
      throw new KsqlException("Failed to insert values into stream/table: "
          + insertValues.getTarget().getSuffix(), e);
    }

    return Optional.empty();
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

    throwOnMissingValue(schema, values);

    final long ts = (long) values.getOrDefault(SchemaUtil.ROWTIME_NAME, clock.getAsLong());
    final Object key = values.get(SchemaUtil.ROWKEY_NAME);

    final GenericRow value = buildValue(schema, values);

    return new RowData(ts, key == null ? null : key.toString(), value);
  }

  private static GenericRow buildValue(
      final LogicalSchema schema,
      final Map<String, Object> values
  ) {
    return new GenericRow(
          schema.withoutImplicitAndKeyFieldsInValue()
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
    final LogicalSchema schema = dataSource.getSchema().withoutImplicitAndKeyFieldsInValue();

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
    final ConnectSchema valueSchema = schema.valueSchema();
    final Map<String, Object> values = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      final String column = columns.get(i);
      final Schema columnSchema = valueSchema.field(column).schema();
      final Expression valueExp = insertValues.getValues().get(i);

      final Object value = new ExpressionResolver(columnSchema, column)
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

  private static void throwOnMissingValue(
      final LogicalSchema schema,
      final Map<String, Object> values
  ) {
    for (final Field field : schema.valueFields()) {
      if (!field.schema().isOptional() && values.getOrDefault(field.name(), null) == null) {
        throw new KsqlException("Got null value for nonnull field: " + field);
      }
    }
  }

  @SuppressWarnings("unchecked") // we know that key is String
  private static byte[] serializeKey(final String keyValue, final DataSource<?> dataSource) {
    try {
      return ((Serde<String>) dataSource.getKeySerdeFactory().create())
          .serializer()
          .serialize(dataSource.getKafkaTopicName(), keyValue);
    } catch (final Exception e) {
      throw new KsqlException("Could not serialize key: " + keyValue, e);
    }
  }

  private static byte[] serializeRow(
      final GenericRow row,
      final DataSource<?> dataSource,
      final KsqlConfig config,
      final ServiceContext serviceContext
  ) {
    final Serde<GenericRow> rowSerde = GenericRowSerDe.from(
        dataSource.getValueSerdeFactory(),
        PhysicalSchema.from(
            dataSource.getSchema().withoutImplicitAndKeyFieldsInValue(),
            dataSource.getSerdeOptions()
        ),
        config,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE);

    try {
      return rowSerde.serializer().serialize(dataSource.getKafkaTopicName(), row);
    } catch (final Exception e) {
      throw new KsqlException("Could not serialize row: " + row, e);
    }
  }

  private static final class RowData {

    final long ts;
    final String key;
    final GenericRow value;

    private RowData(final long ts, final String key, final GenericRow value) {
      this.ts = ts;
      this.key = key;
      this.value = value;
    }
  }

  private static class ExpressionResolver extends AstVisitor<Object, Void> {

    private final Schema fieldSchema;
    private final String fieldName;
    private final SqlValueCoercer defaultSqlValueCoercer = new DefaultSqlValueCoercer();

    ExpressionResolver(final Schema fieldSchema, final String fieldName) {
      this.fieldSchema = Objects.requireNonNull(fieldSchema, "fieldSchema");
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
    }

    @Override
    protected String visitNode(final Node node, final Void context) {
      throw new KsqlException(
          "Only Literals are supported for INSERT INTO. Got: " + node + " for field " + fieldName);
    }

    @Override
    protected Object visitLiteral(final Literal node, final Void context) {
      final Object value = node.getValue();
      if (node instanceof NullLiteral || value == null) {
        return null;
      }

      return defaultSqlValueCoercer.coerce(value, fieldSchema)
          .orElseThrow(
              () -> new KsqlException(
                  "Expected type "
                      + SchemaConverters.logicalToSqlConverter().toSqlType(fieldSchema)
                      + " for field " + fieldName
                      + " but got " + value));
    }
  }
}
