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

package io.confluent.ksql.serde.json;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class JsonSerdeUtilsTest {

  @Test
  public void shouldConvertToBooleanCorrectly() {
    final Boolean b = JsonSerdeUtils.toBoolean(true);
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    JsonSerdeUtils.toBoolean(1);
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(1);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(1L);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger(1.0);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    final Integer i = JsonSerdeUtils.toInteger("1");
    assertThat(i, equalTo(1));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    JsonSerdeUtils.toInteger("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    JsonSerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(1L);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(1);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong(1.0);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    final Long l = JsonSerdeUtils.toLong("1");
    assertThat(l, equalTo(1L));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    JsonSerdeUtils.toLong("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    JsonSerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(1.0);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(1);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble(1L);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    final Double d = JsonSerdeUtils.toDouble("1.0");
    assertThat(d, equalTo(1.0));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    JsonSerdeUtils.toDouble("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    JsonSerdeUtils.toDouble(true);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnUnsupportedSchema() {
    JsonSerdeUtils.isCoercible(ImmutableList.of(1), Schema.OPTIONAL_INT8_SCHEMA);
  }

  @Test
  public void shouldAssumeNullsAreAlwaysCoercible() {
    assertThat(JsonSerdeUtils.isCoercible(null, Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
  }

  @Test
  public void shouldBeCoercibleToInteger() {
    assertThat(JsonSerdeUtils.isCoercible(1, Schema.OPTIONAL_INT32_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(1L, Schema.OPTIONAL_INT32_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4f, Schema.OPTIONAL_INT32_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4, Schema.OPTIONAL_INT32_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible("10", Schema.OPTIONAL_INT32_SCHEMA), is(true));
  }

  @Test
  public void shouldNotBeCoercibleToInteger() {
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(1), Schema.OPTIONAL_INT32_SCHEMA),
        is(false));
  }

  @Test
  public void shouldBeCoercibleToLong() {
    assertThat(JsonSerdeUtils.isCoercible(1, Schema.OPTIONAL_INT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(1L, Schema.OPTIONAL_INT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4f, Schema.OPTIONAL_INT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4, Schema.OPTIONAL_INT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible("10", Schema.OPTIONAL_INT64_SCHEMA), is(true));
  }

  @Test
  public void shouldNotBeCoercibleToLong() {
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(1), Schema.OPTIONAL_INT64_SCHEMA),
        is(false));
  }

  @Test
  public void shouldBeCoercibleToDouble() {
    assertThat(JsonSerdeUtils.isCoercible(1, Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(1L, Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4f, Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4, Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible("10", Schema.OPTIONAL_FLOAT64_SCHEMA), is(true));
  }

  @Test
  public void shouldNotBeCoercibleToDouble() {
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(1), Schema.OPTIONAL_FLOAT64_SCHEMA),
        is(false));
  }

  @Test
  public void shouldBeCoercibleToString() {
    assertThat(JsonSerdeUtils.isCoercible(1, Schema.OPTIONAL_STRING_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(1L, Schema.OPTIONAL_STRING_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4f, Schema.OPTIONAL_STRING_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(0.4, Schema.OPTIONAL_STRING_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible("10", Schema.OPTIONAL_STRING_SCHEMA), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(1), Schema.OPTIONAL_STRING_SCHEMA),
        is(true));
  }

  @Test
  public void shouldBeCoercibleToArray() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.INT32_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(1), schema), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(1L), schema), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(0.4f), schema), is(true));
  }

  @Test
  public void shouldNotBeCoercibleToList() {
    // Given:
    final Schema schema = SchemaBuilder
        .array(Schema.INT32_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(1, schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of(true, 1.0), schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(true, ImmutableList.of(1)), schema),
        is(false));
  }

  @Test
  public void shouldBeCoercibleToMap() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of(true, 1.0), schema), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of(false, 1), schema), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of(true, "str"), schema), is(true));
  }

  @Test
  public void shouldNotBeCoercibleToMap() {
    // Given:
    final Schema schema = SchemaBuilder
        .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(1, schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(true, 1), schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of("string", 1.0), schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of(true, ImmutableList.of()), schema),
        is(false));
  }

  @Test
  public void shouldBeCoercibleToStruct() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of("f0", 1.0), schema), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of("f0", 1), schema), is(true));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of("f0", "str"), schema), is(true));
  }

  @Test
  public void shouldNotBeCoercibleToStruct() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
        .optional()
        .build();

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(1, schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableList.of(true, 1), schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of("not f0", 1.0), schema), is(false));
    assertThat(JsonSerdeUtils.isCoercible(ImmutableMap.of(true, ImmutableList.of()), schema),
        is(false));
  }

  @Test
  public void shouldBeCoercibleAllTheWayDown() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("F0", SchemaBuilder
            .map(
                SchemaBuilder
                    .array(Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build(),
                SchemaBuilder
                    .struct()
                    .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
                    .optional()
                    .build()
            ))
        .optional()
        .build();

    final Object value = ImmutableMap.of(
        "f0", ImmutableMap.of(
            ImmutableList.of("s1", "s2"), ImmutableMap.of("f1", 1),
            ImmutableList.of("s3", "s4"), ImmutableMap.of("f1", 2)
        )
    );

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(value, schema), is(true));
  }

  @Test
  public void shouldNotBeCoercibleIfSomethingDeepIsNot() {
    // Given:
    final Schema schema = SchemaBuilder
        .struct()
        .field("F0", SchemaBuilder
            .map(
                SchemaBuilder
                    .array(Schema.OPTIONAL_STRING_SCHEMA)
                    .optional()
                    .build(),
                SchemaBuilder
                    .struct()
                    .field("f1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                    .optional()
                    .build()
            ))
        .optional()
        .build();

    final Object value = ImmutableMap.of(
        "f0", ImmutableMap.of(
            ImmutableList.of("s1", "s2"), ImmutableMap.of("f1", 1),
            ImmutableList.of("s3", "s4"), ImmutableMap.of("f1", 2)
        )
    );

    // Then:
    assertThat(JsonSerdeUtils.isCoercible(value, schema), is(false));
  }
}
