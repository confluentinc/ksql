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

package io.confluent.ksql.schema.ksql.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.schema.ksql.DataException;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SqlDecimalTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(SqlDecimal.of(10, 2), SqlDecimal.of(10, 2))
        .addEqualityGroup(SqlDecimal.of(11, 2))
        .addEqualityGroup(SqlDecimal.of(10, 3))
        .testEquals();
  }

  @Test
  public void shouldReturnBaseType() {
    MatcherAssert.assertThat(SqlDecimal.of(10, 2).baseType(), Matchers.is(SqlBaseType.DECIMAL));
  }

  @Test
  public void shouldReturnPrecision() {
    assertThat(SqlDecimal.of(10, 2).getPrecision(), is(10));
  }

  @Test
  public void shouldReturnScale() {
    assertThat(SqlDecimal.of(10, 2).getScale(), is(2));
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnInvalidPrecision() {
    SqlDecimal.of(0, 2);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnInvalidScale() {
    SqlDecimal.of(10, -1);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowIfScaleGreaterThanPrecision() {
    SqlDecimal.of(2, 3);
  }

  @Test
  public void shouldImplementToString() {
    assertThat(SqlDecimal.of(10, 2).toString(), is("DECIMAL(10, 2)"));
  }

  @Test
  public void shouldThrowIfValueNotDecimal() {
    // Given:
    final SqlDecimal schema = SqlTypes.decimal(4, 1);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected DECIMAL, got BIGINT");

    // When:
    schema.validateValue(10L);
  }

  @Test
  public void shouldThrowIfValueHasWrongPrecision() {
    // Given:
    final SqlDecimal schema = SqlTypes.decimal(4, 1);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected DECIMAL(4, 1), got precision 5");

    // When:
    schema.validateValue(new BigDecimal("1234.5"));
  }

  @Test
  public void shouldThrowIfValueHasWrongScale() {
    // Given:
    final SqlDecimal schema = SqlTypes.decimal(4, 1);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected DECIMAL(4, 1), got scale 2");

    // When:
    schema.validateValue(new BigDecimal("12.50"));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullValue() {
    // Given:
    final SqlDecimal schema = SqlTypes.decimal(4, 1);

    // When:
    schema.validateValue(null);
  }

  @Test
  public void shouldValidateValue() {
    // Given:
    final SqlDecimal schema = SqlTypes.decimal(4, 1);

    // When:
    schema.validateValue(new BigDecimal("123.0"));
  }

  @Test
  public void shouldResolveDecimalAddition() {
    final Map<Pair<SqlDecimal, SqlDecimal>, SqlDecimal> testCases =
        ImmutableMap.<Pair<SqlDecimal, SqlDecimal>, SqlDecimal>builder()
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 1)), SqlTypes.decimal(3, 1))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 2)), SqlTypes.decimal(4, 2))
            .put(Pair.of(SqlTypes.decimal(2, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(4, 2))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(3, 2)), SqlTypes.decimal(4, 2))
            .put(Pair.of(SqlTypes.decimal(3, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(4, 2))
            .build();

    testCases.forEach((in, expected) -> {
      // When:
      final SqlDecimal result = SqlDecimal.add(in.left, in.right);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldResolveDecimalSubtraction() {
    final Map<Pair<SqlDecimal, SqlDecimal>, SqlDecimal> inputToExpected =
        ImmutableMap.<Pair<SqlDecimal, SqlDecimal>, SqlDecimal>builder()
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 1)), SqlTypes.decimal(3, 1))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 2)), SqlTypes.decimal(4, 2))
            .put(Pair.of(SqlTypes.decimal(2, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(4, 2))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(3, 2)), SqlTypes.decimal(4, 2))
            .put(Pair.of(SqlTypes.decimal(3, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(4, 2))
            .build();

    inputToExpected.forEach((in, expected) -> {
      // When:
      final SqlDecimal result = SqlDecimal.subtract(in.left, in.right);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldResolveDecimalMultiply() {
    final Map<Pair<SqlDecimal, SqlDecimal>, SqlDecimal> inputToExpected =
        ImmutableMap.<Pair<SqlDecimal, SqlDecimal>, SqlDecimal>builder()
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 1)), SqlTypes.decimal(5, 2))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 2)), SqlTypes.decimal(5, 3))
            .put(Pair.of(SqlTypes.decimal(2, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(5, 3))
            .put(Pair.of(SqlTypes.decimal(3, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(6, 3))
            .build();

    inputToExpected.forEach((in, expected) -> {
      // When:
      final SqlDecimal result = SqlDecimal.multiply(in.left, in.right);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldResolveDecimalDivide() {
    final Map<Pair<SqlDecimal, SqlDecimal>, SqlDecimal> inputToExpected =
        ImmutableMap.<Pair<SqlDecimal, SqlDecimal>, SqlDecimal>builder()
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 1)), SqlTypes.decimal(8, 6))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 2)), SqlTypes.decimal(9, 6))
            .put(Pair.of(SqlTypes.decimal(2, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(7, 6))
            .put(Pair.of(SqlTypes.decimal(3, 3), SqlTypes.decimal(3, 3)), SqlTypes.decimal(10, 7))
            .put(Pair.of(SqlTypes.decimal(3, 3), SqlTypes.decimal(3, 2)), SqlTypes.decimal(9, 7))
            .build();

    inputToExpected.forEach((in, expected) -> {
      // When:
      final SqlDecimal result = SqlDecimal.divide(in.left, in.right);

      // Then:
      assertThat(result, is(expected));
    });
  }

  @Test
  public void shouldResolveDecimalMod() {
    final Map<Pair<SqlDecimal, SqlDecimal>, SqlDecimal> inputToExpected =
        ImmutableMap.<Pair<SqlDecimal, SqlDecimal>, SqlDecimal>builder()
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 1)), SqlTypes.decimal(2, 1))
            .put(Pair.of(SqlTypes.decimal(2, 2), SqlTypes.decimal(2, 1)), SqlTypes.decimal(2, 2))
            .put(Pair.of(SqlTypes.decimal(2, 1), SqlTypes.decimal(2, 2)), SqlTypes.decimal(2, 2))
            .put(Pair.of(SqlTypes.decimal(3, 1), SqlTypes.decimal(2, 2)), SqlTypes.decimal(2, 2))
            .build();

    inputToExpected.forEach((in, expected) -> {
      // When:
      final SqlDecimal result = SqlDecimal.modulus(in.left, in.right);

      // Then:
      assertThat(result, is(expected));
    });
  }
}