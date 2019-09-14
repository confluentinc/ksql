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
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultSqlValueCoercerTest {

  private DefaultSqlValueCoercer coercer;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    coercer = new DefaultSqlValueCoercer();
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnArray() {
    coercer.coerce(ImmutableList.of(), SqlTypes.array(SqlTypes.STRING));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnMap() {
    coercer.coerce(ImmutableMap.of(), SqlTypes.map(SqlTypes.STRING));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnStruct() {
    coercer.coerce(new Struct(SchemaBuilder.struct()),
        SqlTypes.struct().field("foo", SqlTypes.STRING).build());
  }

  @Test
  public void shouldCoerceToBoolean() {
    assertThat(coercer.coerce(true, SqlTypes.BOOLEAN), is(Optional.of(true)));
  }

  @Test
  public void shouldNotCoerceToBoolean() {
    assertThat(coercer.coerce("true", SqlTypes.BOOLEAN), is(Optional.empty()));
    assertThat(coercer.coerce(1, SqlTypes.BOOLEAN), is(Optional.empty()));
    assertThat(coercer.coerce(1l, SqlTypes.BOOLEAN), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.BOOLEAN), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.BOOLEAN), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToInteger() {
    assertThat(coercer.coerce(1, SqlTypes.INTEGER), is(Optional.of(1)));
  }

  @Test
  public void shouldNotCoerceToInteger() {
    assertThat(coercer.coerce(true, SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce(1l, SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce("1", SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.INTEGER), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToBigInt() {
    assertThat(coercer.coerce(1, SqlTypes.BIGINT), is(Optional.of(1l)));
    assertThat(coercer.coerce(1l, SqlTypes.BIGINT), is(Optional.of(1l)));
  }

  @Test
  public void shouldNotCoerceToBigInt() {
    assertThat(coercer.coerce(true, SqlTypes.BIGINT), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.BIGINT), is(Optional.empty()));
    assertThat(coercer.coerce("1", SqlTypes.BIGINT), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.BIGINT), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToDouble() {
    assertThat(coercer.coerce(1, SqlTypes.DOUBLE), is(Optional.of(1.0d)));
    assertThat(coercer.coerce(1l, SqlTypes.DOUBLE), is(Optional.of(1.0d)));
    assertThat(coercer.coerce(1.0d, SqlTypes.DOUBLE), is(Optional.of(1.0d)));
  }

  @Test
  public void shouldNotCoerceToDouble() {
    assertThat(coercer.coerce(true, SqlTypes.DOUBLE), is(Optional.empty()));
    assertThat(coercer.coerce("1", SqlTypes.DOUBLE), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.DOUBLE), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToString() {
    assertThat(coercer.coerce("foobar", SqlTypes.STRING), is(Optional.of("foobar")));
  }

  @Test
  public void shouldNotCoerceToString() {
    assertThat(coercer.coerce(true, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(1, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(1l, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.STRING), is(Optional.empty()));
  }


  @Test
  public void shouldCoerceToDecimal() {
    SqlType decimalType = SqlTypes.decimal(2, 1);
    assertThat(coercer.coerce(1, decimalType), is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(1l, decimalType), is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(1.0d, decimalType),
        is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce("1.0", decimalType),
        is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(new BigDecimal("1.0"), decimalType),
        is(Optional.of(new BigDecimal("1.0"))));
  }

  @Test
  public void shouldNotCoerceToDecimal() {
    assertThat(coercer.coerce(true, SqlTypes.STRING), is(Optional.empty()));
  }

  @Test
  public void shouldThrowIfInvalidCoercionString() {
    // Given:
    final String val = "hello";

    // Expect;
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Cannot coerce value to DECIMAL: hello");

    // When:
    coercer.coerce(val, SqlTypes.decimal(2, 1));
  }
}