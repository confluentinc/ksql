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

package io.confluent.ksql.serde.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Test;

public class KafkaSerdeFactoryTest {

  @Test
  public void shouldThroIfMultipleFields() {
    // Given:
    final PersistenceSchema schema = getPersistenceSchema(LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("f1"), SqlTypes.BIGINT)
        .build());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KafkaSerdeFactory.createSerde(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The 'KAFKA' format only supports a single field. Got: [`f0` INTEGER, `f1` BIGINT]"));
  }

  @Test
  public void shouldThroIfBoolean() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.BOOLEAN);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KafkaSerdeFactory.createSerde(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'BOOLEAN'"));
  }

  @Test
  public void shouldThrowIfDecimal() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.decimal(1, 1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KafkaSerdeFactory.createSerde(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'DECIMAL'"));
  }

  @Test
  public void shouldThrowIfArray() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.array(SqlTypes.STRING));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KafkaSerdeFactory.createSerde(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'ARRAY'"));
  }

  @Test
  public void shouldThrowIfMap() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(
        SqlTypes.map(SqlTypes.STRING, SqlTypes.STRING
        ));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KafkaSerdeFactory.createSerde(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'MAP'"));
  }

  @Test
  public void shouldThrowIfStruct() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.struct()
        .field("f0", SqlTypes.STRING)
        .build());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> KafkaSerdeFactory.createSerde(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'STRUCT'"));
  }

  @Test
  public void shouldSerializeNullAsNull() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.INTEGER);

    final Serde<List<?>> serde = KafkaSerdeFactory.createSerde(schema);

    // When:
    final byte[] result = serde.serializer().serialize("topic", null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldDeserializeNullAsNull() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.INTEGER);

    final Serde<List<?>> serde = KafkaSerdeFactory.createSerde(schema);

    // When:
    final Object result = serde.deserializer().deserialize("topic", null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldHandleNullKeyColumn() {
    // Given:
    final LogicalSchema logical = LogicalSchema.builder()
        .valueColumn(ColumnName.of("f0"), SqlTypes.INTEGER)
        .build();

    final PersistenceSchema schema = PhysicalSchema
        .from(logical, SerdeFeatures.of(), SerdeFeatures.of())
        .keySchema();

    final Serde<List<?>> serde = KafkaSerdeFactory.createSerde(schema);

    // When:
    final byte[] bytes = serde.serializer().serialize("topic", null);
    final Object result = serde.deserializer().deserialize("topic", null);

    // Then:
    assertThat(bytes, is(nullValue()));
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldHandleEmptyKey() {
    // Given:
    final LogicalSchema logical = LogicalSchema.builder()
        .valueColumn(ColumnName.of("f0"), SqlTypes.INTEGER)
        .build();

    final PersistenceSchema schema = PhysicalSchema
        .from(logical, SerdeFeatures.of(), SerdeFeatures.of())
        .keySchema();

    final Serde<List<?>> serde = KafkaSerdeFactory.createSerde(schema);

    // When:
    final byte[] bytes = serde.serializer().serialize("topic", ImmutableList.of());
    final Object result = serde.deserializer().deserialize("topic", null);

    // Then:
    assertThat(bytes, is(nullValue()));
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldHandleInt() {
    shouldHandle(SqlTypes.INTEGER, 1);
    shouldHandle(SqlTypes.INTEGER, Integer.MIN_VALUE);
    shouldHandle(SqlTypes.INTEGER, Integer.MAX_VALUE);
  }

  @Test
  public void shouldHandleBigInt() {
    shouldHandle(SqlTypes.BIGINT, 1L);
    shouldHandle(SqlTypes.BIGINT, Long.MIN_VALUE);
    shouldHandle(SqlTypes.BIGINT, Long.MAX_VALUE);
  }

  @Test
  public void shouldHandleDouble() {
    shouldHandle(SqlTypes.DOUBLE, 1.1D);
    shouldHandle(SqlTypes.DOUBLE, Double.MIN_VALUE);
    shouldHandle(SqlTypes.DOUBLE, Double.MAX_VALUE);
    shouldHandle(SqlTypes.DOUBLE, Double.MIN_NORMAL);
  }

  @Test
  public void shouldHandleString() {
    shouldHandle(SqlTypes.STRING, "Yo!");
  }

  private static void shouldHandle(final SqlType fieldSchema, final Object value) {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(fieldSchema);

    final Serde<List<?>> serde = KafkaSerdeFactory.createSerde(schema);

    final List<Object> values = Collections.singletonList(value);

    // When:
    final byte[] bytes = serde.serializer().serialize("topic", values);
    final Object result = serde.deserializer().deserialize("topic", bytes);

    // Then:
    assertThat(result, is(values));
  }

  private static PersistenceSchema schemaWithFieldOfType(final SqlType fieldSchema) {
    final LogicalSchema logical = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), fieldSchema)
        .build();

    return getPersistenceSchema(logical);
  }

  private static PersistenceSchema getPersistenceSchema(final LogicalSchema logical) {
    final PhysicalSchema physicalSchema = PhysicalSchema
        .from(logical, SerdeFeatures.of(), SerdeFeatures.of());
    return physicalSchema.valueSchema();
  }
}