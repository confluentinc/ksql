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

package io.confluent.ksql.schema.ksql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class DefaultSqlValueCoercerTest {

  private DefaultSqlValueCoercer coercer;

  @Before
  public void setUp() {
    coercer = new DefaultSqlValueCoercer();
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnArray() {
    coercer.coerce(ImmutableList.of(), SchemaBuilder.array(Schema.STRING_SCHEMA).build());
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnMap() {
    coercer.coerce(ImmutableMap.of(), SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build());
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnStruct() {
    coercer.coerce(new Struct(SchemaBuilder.struct()), SchemaBuilder.struct().field("foo", Schema.STRING_SCHEMA).build());
  }

  @Test
  public void shouldOnlyCoerceNonNumberTypesToSelf() {
    ImmutableMap.of(
        Schema.OPTIONAL_BOOLEAN_SCHEMA, true,
        Schema.OPTIONAL_STRING_SCHEMA, "self"
    ).forEach((sqlType, value) -> {

      assertThat(coercer.coerce(value, sqlType), is(Optional.of(value)));
      assertThat(coercer.coerce(value, Schema.OPTIONAL_INT32_SCHEMA), is(Optional.empty()));
    });
  }

  @Test
  public void shouldUpCastInt() {
    // Given:
    final int val = 1;

    // Then:
    assertThat(coercer.coerce(val, Schema.OPTIONAL_INT32_SCHEMA), is(Optional.of(1)));
    assertThat(coercer.coerce(val, Schema.OPTIONAL_INT64_SCHEMA), is(Optional.of(1L)));
    assertThat(coercer.coerce(val, Schema.OPTIONAL_FLOAT64_SCHEMA), is(Optional.of(1D)));
  }

  @Test
  public void shouldUpCastBigInt() {
    // Given:
    final long val = 1L;

    // Then:
    assertThat(coercer.coerce(val, Schema.OPTIONAL_INT64_SCHEMA), is(Optional.of(1L)));
    assertThat(coercer.coerce(val, Schema.OPTIONAL_FLOAT64_SCHEMA), is(Optional.of(1D)));
  }

  @Test
  public void shouldUpCastDouble() {
    // Given:
    final double val = 1D;

    // Then:
    assertThat(coercer.coerce(val, Schema.OPTIONAL_FLOAT64_SCHEMA), is(Optional.of(1D)));
  }

  @Test
  public void shouldNotDownCastLong() {
    // Given:
    final long val = 1L;

    // Expect:
    assertThat(coercer.coerce(val, Schema.OPTIONAL_INT32_SCHEMA), is(Optional.empty()));
  }

  @Test
  public void shouldNotDownCastDouble() {
    // Given:
    final double val = 1d;

    // Expect:
    assertThat(coercer.coerce(val, Schema.OPTIONAL_INT32_SCHEMA), is(Optional.empty()));
    assertThat(coercer.coerce(val, Schema.OPTIONAL_INT64_SCHEMA), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceNumberToDecimal() {
    // Given:
    final Object[] values = new Object[]{1, 1L, 1.0d};

    // Expect:
    for (final Object val : values) {
      assertThat(coercer.coerce(val, DecimalUtil.builder(2, 1)), is(Optional.of(new BigDecimal("1.0"))));
    }
  }

  @Test
  public void shouldCoerceStringToDecimal() {
    // Given:
    final String val = "1.0";

    // Expect:
    assertThat(coercer.coerce(val, DecimalUtil.builder(2, 1)), is(Optional.of(new BigDecimal("1.0"))));
  }
}