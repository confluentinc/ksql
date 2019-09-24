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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import io.confluent.ksql.util.DecimalUtil;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.commons.csv.CSVFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class KsqlDelimitedSerializerTest {

  private static final Schema SCHEMA = SchemaBuilder.struct()
      .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ORDERUNITS", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .optional()
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlDelimitedSerializer serializer;

  @Before
  public void setUp() {
    serializer = new KsqlDelimitedSerializer(CSVFormat.DEFAULT.withDelimiter(','));
  }

  @Test
  public void shouldThrowIfNotStruct() {
    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is("DELIMITED does not support anonymous fields")));

    // When:
    serializer.serialize("t1", "not a struct");
  }

  @Test
  public void shouldSerializeRowCorrectly() {
    // Given:
    final Struct data = new Struct(SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0);

    // When:
    final byte[] bytes = serializer.serialize("t1", data);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo("1511897796092,1,item_1,10.0"));
  }

  @Test
  public void shouldSerializeRowWithNull() {
    // Given:
    final Struct data = new Struct(SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", null);

    // When:
    final byte[] bytes = serializer.serialize("t1", data);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo("1511897796092,1,item_1,"));
  }

  @Test
  public void shouldSerializedTopLevelPrimitiveIfValueHasOneField() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    final Struct value = new Struct(schema)
        .put("id", 10L);

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("10"));
  }

  @Test
  public void shouldSerializeDecimal() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", DecimalUtil.builder(4, 2).build())
        .build();

    final Struct value = new Struct(schema)
        .put("id", new BigDecimal("11.12"));

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("11.12"));
  }

  @Test
  public void shouldSerializeDecimalWithPaddedZeros() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", DecimalUtil.builder(4, 2).build())
        .build();

    final Struct value = new Struct(schema)
        .put("id", new BigDecimal("1.12"));

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("01.12"));
  }

  @Test
  public void shouldSerializeZeroDecimalWithPaddedZeros() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", DecimalUtil.builder(4, 2).build())
        .build();

    final Struct value = new Struct(schema)
        .put("id", BigDecimal.ZERO);

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("00.00"));
  }

  @Test
  public void shouldSerializeOneHalfDecimalWithPaddedZeros() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", DecimalUtil.builder(4, 2).build())
        .build();

    final Struct value = new Struct(schema)
        .put("id", new BigDecimal(0.5));

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("00.50"));
  }

  @Test
  public void shouldSerializeNegativeOneHalfDecimalWithPaddedZeros() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", DecimalUtil.builder(4, 2).build())
        .build();

    final Struct value = new Struct(schema)
        .put("id", new BigDecimal(-0.5));

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("\"-00.50\""));
  }

  @Test
  public void shouldSerializeNegativeDecimalWithPaddedZeros() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("id", DecimalUtil.builder(4, 2).build())
        .build();

    final Struct value = new Struct(schema)
        .put("id", new BigDecimal("-1.12"));

    // When:
    final byte[] bytes = serializer.serialize("", value);

    // Then:
    assertThat(new String(bytes, StandardCharsets.UTF_8), is("\"-01.12\""));
  }

  @Test
  public void shouldSerializeRowCorrectlyWithTabDelimeter() {
    shouldSerializeRowCorrectlyWithNonDefaultDelimeter('\t');
  }

  @Test
  public void shouldSerializeRowCorrectlyWithBarDelimeter() {
    shouldSerializeRowCorrectlyWithNonDefaultDelimeter('|');
  }

  private void shouldSerializeRowCorrectlyWithNonDefaultDelimeter(final char delimiter) {
    // Given:
    final Struct data = new Struct(SCHEMA)
        .put("ORDERTIME", 1511897796092L)
        .put("ORDERID", 1L)
        .put("ITEMID", "item_1")
        .put("ORDERUNITS", 10.0);

    final KsqlDelimitedSerializer serializer =
        new KsqlDelimitedSerializer(CSVFormat.DEFAULT.withDelimiter(delimiter));

    // When:
    final byte[] bytes = serializer.serialize("t1", data);

    // Then:
    final String delimitedString = new String(bytes, StandardCharsets.UTF_8);
    assertThat(delimitedString, equalTo(
        "1511897796092" + delimiter +"1" + delimiter + "item_1" + delimiter + "10.0"));
  }

  @Test
  public void shouldThrowOnArrayField() {
    // Given:
    final Schema schemaWithArray = SchemaBuilder.struct()
        .field("f0", SchemaBuilder
            .array(Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build())
        .optional()
        .build();

    final Struct data = new Struct(schemaWithArray)
        .put("f0", null);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is("DELIMITED does not support type: ARRAY")));

    // When:
    serializer.serialize("t1", data);
  }

  @Test
  public void shouldThrowOnMapField() {
    // Given:
    final Schema schemaWithMap = SchemaBuilder.struct()
        .field("f0", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build())
        .optional()
        .build();

    final Struct data = new Struct(schemaWithMap)
        .put("f0", null);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is("DELIMITED does not support type: MAP")));

    // When:
    serializer.serialize("t1", data);
  }

  @Test
  public void shouldThrowOnStructField() {
    // Given:
    final Schema schemaWithStruct = SchemaBuilder.struct()
        .field("f0", SchemaBuilder
            .struct()
            .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build())
        .optional()
        .build();

    final Struct data = new Struct(schemaWithStruct)
        .put("f0", null);

    // Then:
    expectedException.expect(SerializationException.class);
    expectedException.expectCause(hasMessage(is("DELIMITED does not support type: STRUCT")));

    // When:
    serializer.serialize("t1", data);
  }

}
