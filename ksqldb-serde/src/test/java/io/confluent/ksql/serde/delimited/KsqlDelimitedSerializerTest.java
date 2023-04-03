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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeFeatures;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Before;
import org.junit.Test;

public class KsqlDelimitedSerializerTest {

  private static final PersistenceSchema SCHEMA = PersistenceSchema.from(
      LogicalSchema.builder()
          .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
          .valueColumn(ColumnName.of("ORDERID"), SqlTypes.BIGINT)
          .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
          .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.DOUBLE)
          .valueColumn(ColumnName.of("TIME"), SqlTypes.TIME)
          .valueColumn(ColumnName.of("DATE"), SqlTypes.DATE)
          .valueColumn(ColumnName.of("TIMESTAMP"), SqlTypes.TIMESTAMP)
          .valueColumn(ColumnName.of("BYTES"), SqlTypes.BYTES)
          .valueColumn(ColumnName.of("DECIMAL"), SqlTypes.decimal(4,0))
          .build().value(),
      SerdeFeatures.of()
  );

  private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withDelimiter(',');

  private KsqlDelimitedSerializer serializer;

  @Before
  public void setUp() {
    serializer = new KsqlDelimitedSerializer(SCHEMA, CSV_FORMAT);
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    // Given:
    final List<?> values = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Time(100), new Date(864000000L), new Timestamp(100),
        ByteBuffer.wrap(new byte[] {123}), new BigDecimal("10"));

    // When:
    final byte[] bytes = serializer.serialize("t1", values);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo("1511897796092,1,item_1,10.0,100,10,100,ew==,10"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    // Given:
    final List<?> values = Arrays.asList(1511897796092L, 1L, "item_1", null, null, null, null, null, null);

    // When:
    final byte[] bytes = serializer.serialize("t1", values);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo("1511897796092,1,item_1,,,,,,"));
  }

  @Test
  public void shouldSerializeDecimal() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 2));

    final List<?> values = Collections.singletonList(new BigDecimal("11.12"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("11.12"));
  }

  @Test
  public void shouldSerializeDecimalWithPaddedZeros() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 2));

    final List<?> values = Collections.singletonList(new BigDecimal("1.12"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("1.12"));
  }

  @Test
  public void shouldSerializeZeroDecimalWithPaddedZeros() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 2));

    final List<?> values = Collections
        .singletonList(BigDecimal.ZERO.setScale(2, RoundingMode.UNNECESSARY));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("0.00"));
  }

  @Test
  public void shouldSerializeOneHalfDecimalWithPaddedZeros() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 2));

    final List<?> values = Collections.singletonList(new BigDecimal("0.50"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("0.50"));
  }

  @Test
  public void shouldSerializeNegativeOneHalfDecimalWithPaddedZeros() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 2));

    final List<?> values = Collections.singletonList(new BigDecimal("-0.50"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("\"-0.50\""));
  }

  @Test
  public void shouldSerializeNegativeDecimalWithAsStringWithPaddedZeros() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 3));

    final List<?> values = Collections.singletonList(new BigDecimal("-1.120"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("\"-1.120\""));
  }

  @Test
  public void shouldSerializeLargeDecimalWithoutThousandSeparator() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(4, 2));

    final List<?> values = Collections.singletonList(new BigDecimal("1234567890.00"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("1234567890.00"));
  }

  @Test
  public void shouldSerializeReallyLargeDecimalWithoutScientificNotation() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.decimal(10, 3));

    final List<?> values = Collections.singletonList(new BigDecimal("10000000000.000"));

    // When:
    final byte[] bytes = serializer.serialize("", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("10000000000.000"));
  }

  @Test
  public void shouldSerializeRowCorrectlyWithDifferentDelimiter() {
    // Given:
    final List<?> values = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Time(100), new Date(864000000L), new Timestamp(100),
        ByteBuffer.wrap(new byte[] {123}), new BigDecimal("10"));

    final KsqlDelimitedSerializer serializer1 =
        new KsqlDelimitedSerializer(SCHEMA, CSVFormat.DEFAULT.withDelimiter('\t'));

    // When:
    final byte[] bytes = serializer1.serialize("t1", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("1511897796092\t1\titem_1\t10.0\t100\t10\t100\tew==\t10"));
  }

  @Test
  public void shouldThrowOnArrayField() {
    // Given:
    final List<?> values = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Time(100), new Date(864000000L), new Timestamp(100),
        ByteBuffer.wrap(new byte[] {123}), new BigDecimal("10"));

    final KsqlDelimitedSerializer serializer1 =
        new KsqlDelimitedSerializer(SCHEMA, CSVFormat.DEFAULT.withDelimiter('\t'));

    // When:
    final byte[] bytes = serializer1.serialize("t1", values);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("1511897796092\t1\titem_1\t10.0\t100\t10\t100\tew==\t10"));
  }

  @Test
  public void shouldThrowOnInvalidDate() {
    // Given:
    givenSingleColumnSerializer(SqlTypes.DATE);

    final List<?> values = Collections.singletonList(new Date(1234));

    // Then:
    final Exception e = assertThrows(
        SerializationException.class,
        () -> serializer.serialize("", values)
    );
    assertThat(e.getCause().getMessage(),
        is("Date type should not have any time fields set to non-zero values."));
  }

  private void givenSingleColumnSerializer(final SqlType columnType) {
    final PersistenceSchema schema = givenSingleColumnPersistenceSchema(columnType);

    serializer = new KsqlDelimitedSerializer(schema, CSV_FORMAT);
  }

  private static PersistenceSchema givenSingleColumnPersistenceSchema(final SqlType columnType) {
    return PersistenceSchema.from(
        LogicalSchema.builder()
            .valueColumn(ColumnName.of("id"), columnType)
            .build().value(),
        SerdeFeatures.of()
    );
  }
}
