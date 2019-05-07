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
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.rest.entity.KsqlEntity;
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
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

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

    final GenericRow row = extractRow(insertValues, dataSource);
    final byte[] key = serializeKey(row.getColumnValue(SchemaUtil.ROWKEY_INDEX), dataSource);
    final byte[] value = serializeRow(row, dataSource, config, serviceContext);

    final String topicName = dataSource.getKafkaTopicName();

    // for now, just create a new producer each time
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(config.getProducerClientConfigProps());

    producer.send(
        new ProducerRecord<>(
            topicName,
            null,
            row.getColumnValue(SchemaUtil.ROWTIME_INDEX),
            key,
            value
        )
    );

    producer.close(Duration.ofSeconds(MAX_SEND_TIMEOUT_SECONDS));

    return Optional.empty();
  }

  private GenericRow extractRow(
      final InsertValues insertValues,
      final DataSource<?> dataSource
  ) {
    final Optional<String> keyField = dataSource.getKeyField().name();

    final List<String> columns = insertValues.getColumns().isEmpty()
        ? dataSource.getSchema()
          .fields()
          .stream()
          .map(Field::name)
          .filter(name -> !SchemaUtil.ROWTIME_NAME.equals(name))
          .collect(Collectors.toList())
        : insertValues.getColumns();

    if (columns.size() != insertValues.getValues().size()) {
      // this will only happen if insertValues.getColumns() is empty, otherwise
      // the columns/values are verified by the InsertValues constructor
      throw new KsqlException(
          "Expected a value for each column. Expected Columns: " + columns
              + ". Got " + insertValues.getValues());
    }

    final Map<String, Object> values = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      final String column = columns.get(i);
      final Schema columnSchema = dataSource.getSchema().getSchema().field(column).schema();
      final Expression valueExp = insertValues.getValues().get(i);

      values.put(column, new ExpressionResolver(columnSchema, column).process(valueExp, null));
    }

    if (keyField.isPresent()) {
      final String key = keyField.get();
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

    values.putIfAbsent(SchemaUtil.ROWTIME_NAME, clock.getAsLong());

    for (Field field : dataSource.getSchema().fields()) {
      if (!field.schema().isOptional() && values.getOrDefault(field.name(), null) == null) {
        throw new KsqlException("Got null value for nonnull field: " + field);
      }
    }

    return new GenericRow(
        dataSource.getSchema()
            .fields()
            .stream()
            .map(Field::name)
            .map(values::get)
            .collect(Collectors.toList())
    );
  }

  @SuppressWarnings("unchecked") // we know that key is String
  private byte[] serializeKey(final Object keyValue, final DataSource<?> dataSource) {
    if (keyValue == null) {
      return null;
    }

    try {
      return ((Serde<String>) dataSource.getKeySerdeFactory().create())
          .serializer()
          .serialize(dataSource.getKafkaTopicName(), keyValue.toString());
    } catch (final Exception e) {
      throw new KsqlException("Could not serialize key: " + keyValue, e);
    }
  }

  private byte[] serializeRow(
      final GenericRow row,
      final DataSource<?> dataSource,
      final KsqlConfig config,
      final ServiceContext serviceContext
  ) {
    final Serde<GenericRow> rowSerde = dataSource.getKsqlTopicSerde()
        .getGenericRowSerde(
            dataSource.getSchema().getSchema(),
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

  private static class ExpressionResolver extends AstVisitor<Object, Void> {

    private final Schema schema;
    private final String field;


    ExpressionResolver(final Schema schema, final String field) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.field = Objects.requireNonNull(field, "field");
    }

    @Override
    protected String visitNode(final Node node, final Void context) {
      throw new KsqlException(
          "Only Literals are supported for INSERT INTO. Got: " + node + " for field " + field);
    }

    @Override
    protected Object visitLiteral(final Literal node, final Void context) {
      final Object value = node.getValue();
      if (node instanceof NullLiteral || value == null) {
        return null;
      }

      final Type valueType = SchemaUtil.getSchemaFromType(value.getClass()).type();
      if (valueType.equals(schema.type())) {
        return value;
      }

      return SchemaUtil.maybeUpCast(schema.type(), valueType, value)
          .orElseThrow(
              () -> new KsqlException(
                  "Expected type " + schema.type() + " for field " + field
                      + " but got " + value + "(" + valueType + ")")
      );
    }
  }

}
