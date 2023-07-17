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

package io.confluent.ksql.serde.delimited;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlDelimitedDeserializerTest {

  private static final PersistenceSchema ORDER_SCHEMA = persistenceSchema(
      column("ORDERTIME", SqlTypes.BIGINT),
      column("ORDERID", SqlTypes.BIGINT),
      column("ITEMID", SqlTypes.STRING),
      column("ORDERUNITS", SqlTypes.DOUBLE),
      column("COST", SqlTypes.decimal(4, 2))
  );

  private KsqlDelimitedDeserializer deserializer;

  @Before
  public void setUp() {
    deserializer = new KsqlDelimitedDeserializer(ORDER_SCHEMA, CSVFormat.DEFAULT);
  }

  @Test
  public void shouldDeserializeDelimitedCorrectly() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,10.10\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(1511897796092L, 1L, "item_1", 10.0, new BigDecimal("10.10")));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithEmptyFields() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,,\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(1511897796092L, 1L, "item_1", null, null));
  }

  @Test
  public void shouldThrowIfRowHasTooFewColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("t", bytes)
    );

    // Then:
    assertThat(e.getCause().getMessage(),
        is("Column count mismatch on deserialization. topic: t, expected: 5, got: 4"));
  }

  @Test
  public void shouldThrowIfRowHasTooMayColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,10.10,extra\r\n"
        .getBytes(StandardCharsets.UTF_8);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("t", bytes)
    );

    // Then:
    assertThat(e.getCause().getMessage(),
        is("Column count mismatch on deserialization. topic: t, expected: 5, got: 6"));
  }

  @Test
  public void shouldDeserializedTopLevelPrimitiveTypeIfSchemaHasOnlySingleField() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("id", SqlTypes.INTEGER)
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "10".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(10));
  }

  @Test
  public void shouldDeserializeDecimal() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("cost", SqlTypes.decimal(4, 2))
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "01.12".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(new BigDecimal("1.12")));
  }

  @Test
  public void shouldDeserializeDecimalWithoutLeadingZeros() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("cost", SqlTypes.decimal(4, 2))
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "1.12".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(new BigDecimal("1.12")));
  }

  @Test
  public void shouldDeserializeDecimalWithTooSmallScale() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("cost", SqlTypes.decimal(4, 2))
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "2".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(new BigDecimal("2.00")));
  }

  @Test
  public void shouldDeserializeNegativeDecimalSerializedAsNumber() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("cost", SqlTypes.decimal(4, 2))
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "-1.12".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(new BigDecimal("-1.12")));
  }

  @Test
  public void shouldDeserializeNegativeDecimalSerializedAsString() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("cost", SqlTypes.decimal(4, 2))
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "\"-01.12\"".getBytes(StandardCharsets.UTF_8);

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(new BigDecimal("-1.12")));
  }

  @Test
  public void shouldDeserializeDelimitedCorrectlyWithTabDelimiter() {
    // Given:
    final byte[] bytes = "1511897796092\t1\titem_1\t10.0\t10.10\r\n"
        .getBytes(StandardCharsets.UTF_8);

    final KsqlDelimitedDeserializer deserializer =
        new KsqlDelimitedDeserializer(ORDER_SCHEMA, CSVFormat.DEFAULT.withDelimiter('\t'));

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(1511897796092L, 1L, "item_1", 10.0, new BigDecimal("10.10")));
  }

  @Test
  public void shouldDeserializeDelimitedCorrectlyWithBarDelimiter() {
    // Given:
    final byte[] bytes = "1511897796092|1|item_1|10.0|10.10\r\n".getBytes(StandardCharsets.UTF_8);

    final KsqlDelimitedDeserializer deserializer =
        new KsqlDelimitedDeserializer(ORDER_SCHEMA, CSVFormat.DEFAULT.withDelimiter('|'));

    // When:
    final List<?> result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result, contains(1511897796092L, 1L, "item_1", 10.0d, new BigDecimal("10.10")));
  }

  @Test
  public void shouldThrowOnArrayTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("ids", SqlTypes.array(SqlTypes.INTEGER))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createDeserializer(schema)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The 'DELIMITED' format does not support type 'ARRAY', column: `ids`"));
  }

  @Test
  public void shouldThrowOnMapTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("ids", SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createDeserializer(schema)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The 'DELIMITED' format does not support type 'MAP', column: `ids`"));
  }

  @Test
  public void shouldThrowOnStructTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column(
            "ids",
            SqlTypes.struct()
                .field("f0", SqlTypes.INTEGER)
                .build()
        )
    );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> createDeserializer(schema)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The 'DELIMITED' format does not support type 'STRUCT', column: `ids`"));
  }

  private static SimpleColumn column(final String name, final SqlType type) {
    final SimpleColumn column = mock(SimpleColumn.class);
    when(column.name()).thenReturn(ColumnName.of(name));
    when(column.type()).thenReturn(type);
    return column;
  }

  private static PersistenceSchema persistenceSchema(final SimpleColumn... columns) {
    return PersistenceSchema.from(Arrays.asList(columns), SerdeFeatures.of());
  }

  private static KsqlDelimitedDeserializer createDeserializer(final PersistenceSchema schema) {
    return new KsqlDelimitedDeserializer(schema, CSVFormat.DEFAULT.withDelimiter(','));
  }
}
