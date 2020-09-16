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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.EnabledSerdeFeatures;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Struct;
import org.hamcrest.CoreMatchers;
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

  private static final ConnectSchema ORDER_CONNECT_SCHEMA = ConnectSchemas
      .columnsToConnectSchema(ORDER_SCHEMA.columns());

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
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_CONNECT_SCHEMA));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(10.0));
    assertThat(struct.get("COST"), is(new BigDecimal("10.10")));
  }

  @Test
  public void shouldDeserializeJsonCorrectlyWithEmptyFields() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,,\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_CONNECT_SCHEMA));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(nullValue()));
    assertThat(struct.get("COST"), is(nullValue()));
  }

  @Test
  public void shouldThrowIfRowHasTooFewColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("", bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(is("Unexpected field count, csvFields:4 schemaFields:5"))));
  }

  @Test
  public void shouldThrowIfRowHasTooMayColumns() {
    // Given:
    final byte[] bytes = "1511897796092,1,item_1,10.0,10.10,extra\r\n".getBytes(StandardCharsets.UTF_8);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("", bytes)
    );

    // Then:
    assertThat(e.getCause(), (hasMessage(is("Unexpected field count, csvFields:6 schemaFields:5"))));
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
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("id"), CoreMatchers.is(10));
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
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("cost"), is(new BigDecimal("01.12")));
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
    final Struct result = deserializer.deserialize("", bytes);

    // Then:
    assertThat(result.get("cost"), is(new BigDecimal("01.12")));
  }

  @Test
  public void shouldDeserializeDelimitedCorrectlyWithTabDelimiter() {
    // Given:
    final byte[] bytes = "1511897796092\t1\titem_1\t10.0\t10.10\r\n"
        .getBytes(StandardCharsets.UTF_8);

    final KsqlDelimitedDeserializer deserializer =
        new KsqlDelimitedDeserializer(ORDER_SCHEMA, CSVFormat.DEFAULT.withDelimiter('\t'));

    // When:
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_CONNECT_SCHEMA));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(10.0));
    assertThat(struct.get("COST"), is(new BigDecimal("10.10")));
  }

  @Test
  public void shouldDeserializeDelimitedCorrectlyWithBarDelimiter() {
    // Given:
    final byte[] bytes = "1511897796092|1|item_1|10.0|10.10\r\n".getBytes(StandardCharsets.UTF_8);

    final KsqlDelimitedDeserializer deserializer =
        new KsqlDelimitedDeserializer(ORDER_SCHEMA, CSVFormat.DEFAULT.withDelimiter('|'));

    // When:
    final Struct struct = deserializer.deserialize("", bytes);

    // Then:
    assertThat(struct.schema(), is(ORDER_CONNECT_SCHEMA));
    assertThat(struct.get("ORDERTIME"), is(1511897796092L));
    assertThat(struct.get("ORDERID"), is(1L));
    assertThat(struct.get("ITEMID"), is("item_1"));
    assertThat(struct.get("ORDERUNITS"), is(10.0));
    assertThat(struct.get("COST"), is(new BigDecimal("10.10")));
  }

  @Test
  public void shouldThrowOnDeserializedTopLevelPrimitiveWhenSchemaHasMoreThanOneField() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("id", SqlTypes.INTEGER),
        column("id2", SqlTypes.INTEGER)
    );

    final KsqlDelimitedDeserializer deserializer =
        createDeserializer(schema);

    final byte[] bytes = "10".getBytes(UTF_8);

    // When:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> deserializer.deserialize("", bytes)
    );

    // Then:
    assertThat(e.getCause(),
        (instanceOf(KsqlException.class)));
    assertThat(e.getCause(),
        (hasMessage(CoreMatchers.is("Unexpected field count, csvFields:1 schemaFields:2"))));
  }

  @Test
  public void shouldThrowOnArrayTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("ids", SqlTypes.array(SqlTypes.INTEGER))
    );

    // When:
    final Exception e = assertThrows(
        UnsupportedOperationException.class,
        () -> createDeserializer(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("DELIMITED does not support type: ARRAY, field: ids"));
  }

  @Test
  public void shouldThrowOnMapTypes() {
    // Given:
    final PersistenceSchema schema = persistenceSchema(
        column("ids", SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT))
    );

    // When:
    final Exception e = assertThrows(
        UnsupportedOperationException.class,
        () -> createDeserializer(schema)
    );

    // Then:
    assertThat(e.getMessage(), containsString("DELIMITED does not support type: MAP, field: ids"));
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
        UnsupportedOperationException.class,
        () -> createDeserializer(schema)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("DELIMITED does not support type: STRUCT, field: ids"));
  }

  private static SimpleColumn column(final String name, final SqlType type) {
    final SimpleColumn column = mock(SimpleColumn.class);
    when(column.name()).thenReturn(ColumnName.of(name));
    when(column.type()).thenReturn(type);
    return column;
  }

  private static PersistenceSchema persistenceSchema(final SimpleColumn... columns) {
    return PersistenceSchema.from(Arrays.asList(columns), EnabledSerdeFeatures.of());
  }

  private static KsqlDelimitedDeserializer createDeserializer(final PersistenceSchema schema) {
    return new KsqlDelimitedDeserializer(schema, CSVFormat.DEFAULT.withDelimiter(','));
  }
}
