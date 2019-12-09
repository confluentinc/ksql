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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultSqlValueCoercerTest {

  private static final Set<SqlBaseType> UNSUPPORTED = ImmutableSet.of(
      SqlBaseType.ARRAY,
      SqlBaseType.MAP,
      SqlBaseType.STRUCT
  );

  private static final Map<SqlBaseType, SqlType> TYPES = ImmutableMap
      .<SqlBaseType, SqlType>builder()
      .put(SqlBaseType.BOOLEAN, SqlTypes.BOOLEAN)
      .put(SqlBaseType.INTEGER, SqlTypes.INTEGER)
      .put(SqlBaseType.BIGINT, SqlTypes.BIGINT)
      .put(SqlBaseType.DECIMAL, SqlTypes.decimal(2, 1))
      .put(SqlBaseType.DOUBLE, SqlTypes.DOUBLE)
      .put(SqlBaseType.STRING, SqlTypes.STRING)
      .put(SqlBaseType.ARRAY, SqlArray.of(SqlTypes.BIGINT))
      .put(SqlBaseType.MAP, SqlMap.of(SqlTypes.BIGINT))
      .build();

  private static final Map<SqlBaseType, Object> INSTANCES = ImmutableMap
      .<SqlBaseType, Object>builder()
      .put(SqlBaseType.BOOLEAN, false)
      .put(SqlBaseType.INTEGER, 1)
      .put(SqlBaseType.BIGINT, 2L)
      .put(SqlBaseType.DECIMAL, BigDecimal.ONE)
      .put(SqlBaseType.DOUBLE, 3.0D)
      .put(SqlBaseType.STRING, "4.1")
      .put(SqlBaseType.ARRAY, ImmutableList.of(1L, 2L))
      .put(SqlBaseType.MAP, ImmutableMap.of("foo", 1L))
      .build();

  private DefaultSqlValueCoercer coercer;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() {
    coercer = DefaultSqlValueCoercer.INSTANCE;
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
    assertThat(coercer.coerce(1L, SqlTypes.BOOLEAN), is(Optional.empty()));
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
    assertThat(coercer.coerce(1L, SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce("1", SqlTypes.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.INTEGER), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToBigInt() {
    assertThat(coercer.coerce(1, SqlTypes.BIGINT), is(Optional.of(1L)));
    assertThat(coercer.coerce(1L, SqlTypes.BIGINT), is(Optional.of(1L)));
  }

  @Test
  public void shouldNotCoerceToBigInt() {
    assertThat(coercer.coerce(true, SqlTypes.BIGINT), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.BIGINT), is(Optional.empty()));
    assertThat(coercer.coerce("1", SqlTypes.BIGINT), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.BIGINT), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToDecimal() {
    final SqlType decimalType = SqlTypes.decimal(2, 1);
    assertThat(coercer.coerce(1, decimalType), is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(1L, decimalType), is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce("1.0", decimalType), is(Optional.of(new BigDecimal("1.0"))));
    assertThat(coercer.coerce(new BigDecimal("1.0"), decimalType),
        is(Optional.of(new BigDecimal("1.0"))));
  }

  @Test
  public void shouldNotCoerceToDecimal() {
    final SqlType decimalType = SqlTypes.decimal(2, 1);
    assertThat(coercer.coerce(true, decimalType), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, decimalType), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToDouble() {
    assertThat(coercer.coerce(1, SqlTypes.DOUBLE), is(Optional.of(1.0d)));
    assertThat(coercer.coerce(1L, SqlTypes.DOUBLE), is(Optional.of(1.0d)));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.DOUBLE), is(Optional.of(123.0d)));
    assertThat(coercer.coerce(1.0d, SqlTypes.DOUBLE), is(Optional.of(1.0d)));
  }

  @Test
  public void shouldNotCoerceToDouble() {
    assertThat(coercer.coerce(true, SqlTypes.DOUBLE), is(Optional.empty()));
    assertThat(coercer.coerce("1", SqlTypes.DOUBLE), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToArray() {
    final SqlType arrayType = SqlTypes.array(SqlTypes.DOUBLE);
    assertThat(coercer.coerce(ImmutableList.of(1), arrayType), is(Optional.of(ImmutableList.of(1d))));
    assertThat(coercer.coerce(ImmutableList.of(1L), arrayType), is(Optional.of(ImmutableList.of(1d))));
    assertThat(coercer.coerce(ImmutableList.of(1.1), arrayType), is(Optional.of(ImmutableList.of(1.1d))));
  }

  @Test
  public void shouldNotCoerceToArray() {
    final SqlType arrayType = SqlTypes.array(SqlTypes.DOUBLE);
    assertThat(coercer.coerce(true, arrayType), is(Optional.empty()));
    assertThat(coercer.coerce(1L, arrayType), is(Optional.empty()));
    assertThat(coercer.coerce("foo", arrayType), is(Optional.empty()));
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1), arrayType), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToMap() {
    final SqlType mapType = SqlTypes.map(SqlTypes.DOUBLE);
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1), mapType), is(Optional.of(ImmutableMap.of("foo", 1d))));
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1L), mapType), is(Optional.of(ImmutableMap.of("foo", 1d))));
    assertThat(coercer.coerce(ImmutableMap.of("foo", 1.1), mapType), is(Optional.of(ImmutableMap.of("foo", 1.1d))));
  }

  @Test
  public void shouldNotCoerceToMap() {
    final SqlType mapType = SqlTypes.map(SqlTypes.DOUBLE);
    assertThat(coercer.coerce(true, mapType), is(Optional.empty()));
    assertThat(coercer.coerce(1L, mapType), is(Optional.empty()));
    assertThat(coercer.coerce("foo", mapType), is(Optional.empty()));
    assertThat(coercer.coerce(ImmutableList.of("foo"), mapType), is(Optional.empty()));
  }

  @Test
  public void shouldCoerceToString() {
    assertThat(coercer.coerce("foobar", SqlTypes.STRING), is(Optional.of("foobar")));
  }

  @Test
  public void shouldNotCoerceToString() {
    assertThat(coercer.coerce(true, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(1, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(1L, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(1.0d, SqlTypes.STRING), is(Optional.empty()));
    assertThat(coercer.coerce(new BigDecimal(123), SqlTypes.STRING), is(Optional.empty()));
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

  @Test
  public void shouldCoerceUsingSameRulesAsBaseTypeUpCastRules() {
    for (final SqlBaseType fromBaseType : supportedTypes()) {
      // Given:
      final Map<Boolean, List<SqlBaseType>> partitioned = supportedTypes().stream()
          .collect(Collectors
              .partitioningBy(toBaseType -> coercionShouldBeSupported(fromBaseType, toBaseType)));

      final List<SqlBaseType> shouldUpCast = partitioned.getOrDefault(true, ImmutableList.of());
      final List<SqlBaseType> shouldNotUpCast = partitioned.getOrDefault(false, ImmutableList.of());

      // Then:
      shouldUpCast.forEach(toBaseType -> assertThat(
          "should coerce " + fromBaseType + " to " + toBaseType,
          coercer.coerce(getInstance(fromBaseType), getType(toBaseType)),
          is(not(Optional.empty()))
      ));

      shouldNotUpCast.forEach(toBaseType -> assertThat(
          "should not coerce " + fromBaseType + " to " + toBaseType,
          coercer.coerce(getInstance(fromBaseType), getType(toBaseType)),
          is(Optional.empty())
      ));
    }
  }

  private static boolean coercionShouldBeSupported(
      final SqlBaseType fromBaseType,
      final SqlBaseType toBaseType
  ) {
    if (fromBaseType == SqlBaseType.STRING && toBaseType == SqlBaseType.DECIMAL) {
      // Handled by parsing the string to a decimal:
      return true;
    }
    return fromBaseType.canImplicitlyCast(toBaseType);
  }

  private static List<SqlBaseType> supportedTypes() {
    return Arrays.stream(SqlBaseType.values())
        .filter(baseType -> !UNSUPPORTED.contains(baseType))
        .collect(Collectors.toList());
  }

  private static Object getInstance(final SqlBaseType baseType) {
    final Object instance = INSTANCES.get(baseType);
    assertThat(
        "invalid test: need instance for base type:" + baseType,
        instance,
        is(notNullValue())
    );
    return instance;
  }

  private static SqlType getType(final SqlBaseType baseType) {
    final SqlType type = TYPES.get(baseType);
    assertThat(
        "invalid test: need type for base type:" + baseType,
        type,
        is(notNullValue())
    );
    return type;
  }
}