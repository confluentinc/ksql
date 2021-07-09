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

package io.confluent.ksql.schema;

import static io.confluent.ksql.schema.Operator.ADD;
import static io.confluent.ksql.schema.Operator.DIVIDE;
import static io.confluent.ksql.schema.Operator.MODULUS;
import static io.confluent.ksql.schema.Operator.MULTIPLY;
import static io.confluent.ksql.schema.Operator.SUBTRACT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BOOLEAN;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BYTES;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DATE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.TIME;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import org.junit.Test;

public class OperatorTest {

  private static final SqlDecimal DECIMAL = SqlTypes.decimal(2, 1);
  private static final SqlDecimal INT_AS_DECIMAL = SqlTypes.decimal(10, 0);
  private static final SqlDecimal BIGINT_AS_DECIMAL = SqlTypes.decimal(19, 0);

  private static final Map<SqlBaseType, SqlType> TYPES = ImmutableMap
      .<SqlBaseType, SqlType>builder()
      .put(SqlBaseType.BOOLEAN, BOOLEAN)
      .put(SqlBaseType.INTEGER, INTEGER)
      .put(SqlBaseType.BIGINT, BIGINT)
      .put(SqlBaseType.DECIMAL, SqlTypes.decimal(2, 1))
      .put(SqlBaseType.DOUBLE, DOUBLE)
      .put(SqlBaseType.STRING, STRING)
      .put(SqlBaseType.TIME, TIME)
      .put(SqlBaseType.DATE, DATE)
      .put(SqlBaseType.TIMESTAMP, TIMESTAMP)
      .put(SqlBaseType.BYTES, BYTES)
      .put(SqlBaseType.ARRAY, SqlTypes.array(BIGINT))
      .put(SqlBaseType.MAP, SqlTypes.map(SqlTypes.STRING, INTEGER))
      .put(SqlBaseType.STRUCT, SqlTypes.struct().field("f", INTEGER).build())
      .build();

  @Test
  public void shouldResolveValidAddReturnType() {
    assertThat(ADD.resultType(STRING, STRING), is(STRING));

    assertConversionRule(ADD, SqlDecimal::add);
  }

  @Test
  public void shouldResolveSubtractReturnType() {
    assertConversionRule(SUBTRACT, SqlDecimal::subtract);
  }

  @Test
  public void shouldResolveMultiplyReturnType() {
    assertConversionRule(MULTIPLY, SqlDecimal::multiply);
  }

  @Test
  public void shouldResolveDivideReturnType() {
    assertConversionRule(DIVIDE, SqlDecimal::divide);
  }

  @Test
  public void shouldResolveModulusReturnType() {
    assertConversionRule(MODULUS, SqlDecimal::modulus);
  }

  @Test
  public void shouldThrowExceptionWhenNullType() {
    allOperations().forEach(op -> {
      for (final SqlBaseType leftBaseType : SqlBaseType.values()) {
        // When:
        final Throwable exception = assertThrows(KsqlException.class,
                () -> op.resultType(getType(leftBaseType), null));

        // Then:
        assertEquals(String.format("Arithmetic on types %s and null are not supported.",
                getType(leftBaseType)), exception.getMessage());
      }
    });

    allOperations().forEach(op -> {
      for (final SqlBaseType rightBaseType : SqlBaseType.values()) {
        // When:
        final Throwable exception = assertThrows(KsqlException.class,
                () -> op.resultType(null, getType(rightBaseType)));

        // Then:
        assertEquals(String.format("Arithmetic on types null and %s are not supported.",
                getType(rightBaseType)), exception.getMessage());
      }
    });
  }

  @Test
  public void shouldWorkUsingSameRulesAsBaseTypeUpCastRules() {
    allOperations().forEach(op -> {

      for (final SqlBaseType leftBaseType : SqlBaseType.values()) {
        // Given:
        final Map<Boolean, List<SqlBaseType>> partitioned = Arrays
            .stream(SqlBaseType.values())
            .collect(Collectors.partitioningBy(
                rightBaseType -> shouldBeSupported(op, leftBaseType, rightBaseType)));

        final List<SqlBaseType> shouldUpCast = partitioned.getOrDefault(true, ImmutableList.of());
        final List<SqlBaseType> shouldNotUpCast = partitioned
            .getOrDefault(false, ImmutableList.of());

        // Then:
        shouldUpCast.forEach(rightBaseType ->
            assertThat(
                "should support " + op + " on (" + leftBaseType + ", " + rightBaseType + ")",
                op.resultType(getType(leftBaseType), getType(rightBaseType)),
                is(notNullValue())
            )
        );

        shouldNotUpCast.forEach(rightBaseType -> {
          try {
            op.resultType(getType(leftBaseType), getType(rightBaseType));
            fail("should not support " + op + " on (" + leftBaseType + ", " + rightBaseType + ")");
          } catch (final KsqlException e) {
            // Expected
          }
        });
      }
    });
  }

  private static void assertConversionRule(
      final Operator op,
      final BinaryOperator<SqlDecimal> binaryResolver
  ) {
    assertThat(op.resultType(INTEGER, INTEGER), is(INTEGER));
    assertThat(op.resultType(INTEGER, BIGINT), is(BIGINT));
    assertThat(op.resultType(BIGINT, INTEGER), is(BIGINT));
    assertThat(op.resultType(INTEGER, DECIMAL), is(binaryResolver.apply(INT_AS_DECIMAL, DECIMAL)));
    assertThat(op.resultType(DECIMAL, INTEGER), is(binaryResolver.apply(DECIMAL, INT_AS_DECIMAL)));
    assertThat(op.resultType(INTEGER, DOUBLE), is(DOUBLE));
    assertThat(op.resultType(DOUBLE, INTEGER), is(DOUBLE));

    assertThat(op.resultType(BIGINT, BIGINT), is(BIGINT));
    assertThat(op.resultType(BIGINT, DECIMAL), is(binaryResolver.apply(BIGINT_AS_DECIMAL, DECIMAL)));
    assertThat(op.resultType(DECIMAL, BIGINT), is(binaryResolver.apply(DECIMAL, BIGINT_AS_DECIMAL)));
    assertThat(op.resultType(BIGINT, DOUBLE), is(DOUBLE));
    assertThat(op.resultType(DOUBLE, BIGINT), is(DOUBLE));

    assertThat(op.resultType(DECIMAL, DECIMAL), is(binaryResolver.apply(DECIMAL, DECIMAL)));
    assertThat(op.resultType(DECIMAL, DOUBLE), is(DOUBLE));
    assertThat(op.resultType(DOUBLE, DECIMAL), is(DOUBLE));

    assertThat(op.resultType(DOUBLE, DOUBLE), is(DOUBLE));
  }

  private static boolean shouldBeSupported(
      final Operator op,
      final SqlBaseType leftBaseType,
      final SqlBaseType rightBaseType
  ) {
    return (isNumeric(leftBaseType) && isNumeric(rightBaseType))
        || (op == ADD && leftBaseType == SqlBaseType.STRING && rightBaseType == SqlBaseType.STRING);
  }

  private static boolean isNumeric(final SqlBaseType baseType) {
    switch (baseType) {
      case INTEGER:
      case BIGINT:
      case DECIMAL:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  private static List<Operator> allOperations() {
    return ImmutableList.copyOf(Operator.values());
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