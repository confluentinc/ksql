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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSerdeFactoryTest {

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  private KafkaSerdeFactory factory;

  @Before
  public void setUp() {
    factory = new KafkaSerdeFactory();
  }

  @Test
  public void shouldThrowOnValidateIfMultipleFields() {
    // Given:
    final PersistenceSchema schema = getPersistenceSchema(LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("f1"), SqlTypes.BIGINT)
        .build());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "The 'KAFKA' format only supports a single field. Got: f0 INT, f1 BIGINT"));
  }

  @Test
  public void shouldThrowOnValidateIfBoolean() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.BOOLEAN);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'BOOLEAN'"));
  }

  @Test
  public void shouldThrowOnValidateIfDecimal() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.decimal(1, 1));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'DECIMAL'"));
  }

  @Test
  public void shouldThrowOnValidateIfArray() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.array(SqlTypes.STRING));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'ARRAY'"));
  }

  @Test
  public void shouldThrowOnValidateIfMap() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.map(SqlTypes.STRING));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'MAP'"));
  }

  @Test
  public void shouldThrowOnValidateIfStruct() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.struct()
        .field("f0", SqlTypes.STRING)
        .build());

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> factory.validate(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("The 'KAFKA' format does not support type 'STRUCT'"));
  }

  @Test
  public void shouldSerializeNullAsNull() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.INTEGER);

    final Serde<Object> serde = factory.createSerde(schema, ksqlConfig, srClientFactory);

    // When:
    final byte[] result = serde.serializer().serialize("topic", null);

    // Then:
    assertThat(result, is(nullValue()));
  }

  @Test
  public void shouldDeserializeNullAsNull() {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(SqlTypes.INTEGER);

    final Serde<Object> serde = factory.createSerde(schema, ksqlConfig, srClientFactory);

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

    final PersistenceSchema schema = PhysicalSchema.from(logical, SerdeOptions.of()).keySchema();

    final Serde<Object> serde = factory.createSerde(schema, ksqlConfig, srClientFactory);

    // Given:
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

    final PersistenceSchema schema = PhysicalSchema.from(logical, SerdeOptions.of()).keySchema();

    final Serde<Object> serde = factory.createSerde(schema, ksqlConfig, srClientFactory);

    // Given:
    final byte[] bytes = serde.serializer().serialize("topic", new Struct(logical.keyConnectSchema()));
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

  private void shouldHandle(final SqlType fieldSchema, final Object value) {
    // Given:
    final PersistenceSchema schema = schemaWithFieldOfType(fieldSchema);

    factory.validate(schema);
    final Serde<Object> serde = factory.createSerde(schema, ksqlConfig, srClientFactory);

    final Struct struct = new Struct(schema.serializedSchema());
    struct.put("f0", value);

    // When:
    final byte[] bytes = serde.serializer().serialize("topic", struct);
    final Object result = serde.deserializer().deserialize("topic", bytes);

    // Then:
    assertThat(result, is(struct));
  }

  private static PersistenceSchema schemaWithFieldOfType(final SqlType fieldSchema) {
    final LogicalSchema logical = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), fieldSchema)
        .build();

    return getPersistenceSchema(logical);
  }

  private static PersistenceSchema getPersistenceSchema(final LogicalSchema logical) {
    final PhysicalSchema physicalSchema = PhysicalSchema.from(logical, SerdeOptions.of());
    return physicalSchema.valueSchema();
  }
}