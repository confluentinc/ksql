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

package io.confluent.ksql.test.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlType;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.test.serde.string.StringSerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FakeInsertValuesExecutor {
  public static void execute(
          KsqlExecutionContext executionContext,
          KsqlConfig ksqlConfig,
          FakeKafkaService fakeKafkaService,
          InsertValues insertValues) {
    final DataSource<?> dataSource = executionContext
            .getMetaStore()
            .getSource(insertValues.getTarget().toString());
    if (dataSource == null) {
      throw new KsqlException("Cannot insert values into an unknown stream/table: "
              + insertValues.getTarget().getSuffix());
    }
    final Topic topic = fakeKafkaService.getTopic(dataSource.getKsqlTopic().getKafkaTopicName());
    fakeKafkaService.writeRecord(
            topic.getName(),
            FakeKafkaRecord.of(
                    buildInsertIntoRecord(
                            insertValues,
                            dataSource,
                            ksqlConfig,
                            executionContext,
                            topic),null));
  }

  private static Record buildInsertIntoRecord(
          InsertValues insertValues,
          DataSource<?> dataSource,
          KsqlConfig ksqlConfig,
          KsqlExecutionContext executionContext,
          Topic topic) {
    final List<Expression> values = insertValues.getValues();
    final List<String> insertIntoColumns = insertValues.getColumns();
    final List<String> columns = dataSource.getSchema()
            .withoutImplicitFields()
            .fields()
            .stream()
            .map(field -> field.name())
            .collect(Collectors.toList());

    GenericRow row = valuesToRow(
            values,
            insertIntoColumns,
            columns,
            dataSource.getSchema().withoutImplicitFields().fields());

    final String keyField = dataSource.getKeyField().name().get();
    return new Record(
            topic,
            row.getColumnValue(columns.indexOf(keyField)).toString(),
            serializeRow(row, dataSource, ksqlConfig, executionContext, topic),
            0,
            null
    );
  }

  private static GenericRow valuesToRow(
          List<Expression> values,
          List<String> insertIntoColumns,
          List<String> columns,
          List<Field> fields) {
    if (values.size() > columns.size()) {
      throw new KsqlException("Inserting " + values.size() + " values into "
              + columns.size() + " columns");
    }

    List<Object> row = new ArrayList<>();

    for (int i = 0; i < columns.size(); i++) {
      int index = insertIntoColumns.indexOf(columns.get(i));
      if (insertIntoColumns.isEmpty()) {
        index = i;
      }
      if (index >= 0) {
        final Object value = ((Literal) values.get(index)).getValue();
        final SqlType expectedType = SchemaConverters
                .logicalToSqlConverter()
                .toSqlType(fields.get(i).schema())
                .getSqlType();

        Optional<Number> upcasted = new DefaultSqlValueCoercer().coerce(value, expectedType);
        if (upcasted.isPresent()) {
          row.add(upcasted.get());
        } else {
          throw new KsqlException("Expected type " + expectedType + " for field "
                  + columns.get(i) + " but got " + value
                  + "(" + SchemaUtil.getSchemaFromType(value.getClass()).type() + ")");
        }
      } else {
        row.add(null);
      }
    }
    return new GenericRow(row);
  }

  private static Object serializeRow(
          GenericRow row,
          DataSource<?> dataSource,
          KsqlConfig ksqlConfig,
          KsqlExecutionContext executionContext,
          Topic topic) {
    final Serde<GenericRow> rowSerde = GenericRowSerDe.from(
            dataSource.getValueSerdeFactory(),
            PhysicalSchema.from(
                    dataSource.getSchema().withoutImplicitFields(),
                    dataSource.getSerdeOptions()
            ),
            ksqlConfig,
            executionContext.getServiceContext().getSchemaRegistryClientFactory(),
            "",
            NoopProcessingLogContext.INSTANCE);

    final byte[] bytes = rowSerde.serializer().serialize(topic.getName(), row);
    if (topic.getValueSerdeSupplier() instanceof StringSerdeSupplier) {
      return new String(bytes);
    } else {
      try {
        return new ObjectMapper().readValue(bytes, Object.class);
      } catch (final IOException e) {
        throw new InvalidFieldException("value", "failed to parse", e);
      }
    }
  }
}
