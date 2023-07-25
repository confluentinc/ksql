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

import static io.confluent.ksql.schema.ksql.types.SqlTypes.BIGINT;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BOOLEAN;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.BYTES;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DATE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.DOUBLE;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.INTEGER;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.STRING;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.TIME;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.TIMESTAMP;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.array;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.decimal;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.map;
import static io.confluent.ksql.schema.ksql.types.SqlTypes.struct;
import static java.math.BigDecimal.ONE;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.confluent.ksql.schema.ksql.SqlValueCoercer.Result;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class DefaultSqlValueCoercerTest {

  // Tests coercion of specific values to different types.
  @SuppressWarnings("UnstableApiUsage")
  @RunWith(Parameterized.class)
  public static class LaxValueCoercionTest {

    // Map of value to coerce to a map of per-type expected responses.
    // Any SqlBaseType not in the responses will implicitly expect a failure response.
    public static final Map<Object, Map<SqlType, Object>> TEST_CASES = ImmutableMap
        .<Object, Map<SqlType, Object>>builder()
        // BOOLEAN:
        .put(true, ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, true)
            .put(STRING, laxOnly("true"))
            .build()
        )
        .put(false, ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, false)
            .put(STRING, laxOnly("false"))
            .build()
        )
        // INT:
        .put(101, ImmutableMap.<SqlType, Object>builder()
            .put(INTEGER, 101)
            .put(BIGINT, 101L)
            .put(decimal(3, 0), new BigDecimal("101"))
            .put(DOUBLE, 101.0D)
            .put(STRING, laxOnly("101"))
            .build()
        )
        .put(-99, ImmutableMap.<SqlType, Object>builder()
            .put(INTEGER, -99)
            .put(BIGINT, -99L)
            .put(decimal(4, 1), new BigDecimal("-99.0"))
            .put(DOUBLE, -99.0D)
            .put(STRING, laxOnly("-99"))
            .build()
        )
        // BIGINT:
        .put(3L, ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, 3L)
            .put(decimal(3, 2), new BigDecimal("3.00"))
            .put(DOUBLE, 3.0D)
            .put(STRING, laxOnly("3"))
            .build()
        )
        .put(Integer.MIN_VALUE - 1L, ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, -2147483649L)
            .put(decimal(10, 0), new BigDecimal("-2147483649"))
            .put(DOUBLE, -2147483649.0D)
            .put(STRING, laxOnly("-2147483649"))
            .build()
        )
        .put(Integer.MAX_VALUE + 1L, ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, 2147483648L)
            .put(decimal(11, 1), new BigDecimal("2147483648.0"))
            .put(DOUBLE, 2147483648.0D)
            .put(STRING, laxOnly("2147483648"))
            .build()
        )
        // DECIMAL:
        .put(new BigDecimal("10.010"), ImmutableMap.<SqlType, Object>builder()
            .put(decimal(4, 2), new BigDecimal("10.01"))
            .put(decimal(9, 3), new BigDecimal("10.010"))
            .put(decimal(4, 1), Result.failure())
            .put(DOUBLE, 10.01D)
            .put(STRING, laxOnly("10.010"))
            .build()
        )
        .put(new BigDecimal(Long.MIN_VALUE).subtract(ONE), ImmutableMap
            .<SqlType, Object>builder()
            .put(decimal(19, 0), new BigDecimal("-9223372036854775809"))
            .put(DOUBLE, -9223372036854775809.0D)
            .put(STRING, laxOnly("-9223372036854775809"))
            .build()
        )
        // DOUBLE:
        .put(12.34D, ImmutableMap.<SqlType, Object>builder()
            .put(DOUBLE, 12.34D)
            .put(STRING, laxOnly("12.34"))
            .build()
        )
        // STRING:
        .put("TrUe", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(true))
            .put(STRING, "TrUe")
            .put(BYTES, ByteBuffer.wrap(new byte[] {78, (byte) 181, 30}))
            .build()
        )
        .put("FaLse", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(false))
            .put(STRING, "FaLse")
            .build()
        )
        .put("YeS", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(true))
            .put(STRING, "YeS")
            .put(BYTES, ByteBuffer.wrap(new byte[] {97, (byte) 228}))
            .build()
        )
        .put("nO", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(false))
            .put(STRING, "nO")
            .put(BYTES, ByteBuffer.wrap(new byte[] {(byte) 156}))
            .build()
        )
        .put("t", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(true))
            .put(STRING, "t")
            .build()
        )
        .put("F", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(false))
            .put(STRING, "F")
            .build()
        )
        .put("Y", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(true))
            .put(STRING, "Y")
            .build()
        )
        .put("n", ImmutableMap.<SqlType, Object>builder()
            .put(BOOLEAN, laxOnly(false))
            .put(STRING, "n")
            .build()
        )
        .put(String.valueOf(Integer.MIN_VALUE), ImmutableMap.<SqlType, Object>builder()
            .put(INTEGER, laxOnly(-2147483648))
            .put(BIGINT, laxOnly(-2147483648L))
            .put(decimal(10, 0), laxOnly(new BigDecimal("-2147483648")))
            .put(DOUBLE, laxOnly(-2147483648.0D))
            .put(STRING, "-2147483648")
            .build()
        )
        .put(String.valueOf(Integer.MAX_VALUE), ImmutableMap.<SqlType, Object>builder()
            .put(INTEGER, laxOnly(2147483647))
            .put(BIGINT, laxOnly(2147483647L))
            .put(decimal(10, 0), laxOnly(new BigDecimal("2147483647")))
            .put(DOUBLE, laxOnly(2147483647.0D))
            .put(STRING, "2147483647")
            .put(BYTES, ByteBuffer.wrap(new byte[] {-37,94,59,-29,-51,-6,-29}))
            .build()
        )
        .put(String.valueOf(Integer.MIN_VALUE - 1L), ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, laxOnly(-2147483649L))
            .put(decimal(10, 0), laxOnly(new BigDecimal("-2147483649")))
            .put(DOUBLE, laxOnly(-2147483649.0D))
            .put(STRING, "-2147483649")
            .build()
        )
        .put(String.valueOf(Integer.MAX_VALUE + 1L), ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, laxOnly(2147483648L))
            .put(decimal(10, 0), laxOnly(new BigDecimal("2147483648")))
            .put(DOUBLE, laxOnly(2147483648.0D))
            .put(STRING, "2147483648")
            .put(BYTES, ByteBuffer.wrap(new byte[] {(byte) 219, 94, 59, (byte) 227, (byte) 205,
                (byte) 250, (byte) 227}))
            .build()
        )
        .put(String.valueOf(Long.MIN_VALUE), ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, laxOnly(-9223372036854775808L))
            .put(decimal(19, 0), laxOnly(new BigDecimal("-9223372036854775808")))
            .put(DOUBLE, laxOnly(-9223372036854775808.0D))
            .put(STRING, "-9223372036854775808")
            .build()
        )
        .put(String.valueOf(Long.MAX_VALUE), ImmutableMap.<SqlType, Object>builder()
            .put(BIGINT, laxOnly(9223372036854775807L))
            .put(decimal(19, 0), laxOnly(new BigDecimal("9223372036854775807")))
            .put(DOUBLE, laxOnly(9223372036854775807.0D))
            .put(STRING, "9223372036854775807")
            .put(BYTES, ByteBuffer.wrap(new byte[] {-9,109,-73,-33,-67,-76,-33,-81,57,-29,-66,-7,-13,78}))
            .build()
        )
        .put(String.valueOf(new BigDecimal(Long.MIN_VALUE).subtract(ONE)), ImmutableMap
            .<SqlType, Object>builder()
            .put(decimal(19, 0), laxOnly(new BigDecimal("-9223372036854775809")))
            .put(DOUBLE, laxOnly(-9223372036854775809.0D))
            .put(STRING, "-9223372036854775809")
            .build()
        )
        .put(String.valueOf(new BigDecimal(Long.MAX_VALUE).add(ONE)),
            ImmutableMap.<SqlType, Object>builder()
                .put(decimal(19, 0), laxOnly(new BigDecimal("9223372036854775808")))
                .put(DOUBLE, laxOnly(9223372036854775808.0D))
                .put(STRING, "9223372036854775808")
                .put(BYTES, ByteBuffer.wrap(new byte[] {(byte) 247, 109, (byte) 183, (byte) 223,
                    (byte) 189, (byte) 180, (byte) 223, (byte) 175, 57, (byte) 227, (byte) 190,
                    (byte) 249, (byte) 243, 79}))
                .build()
        )
        .put("\t 10 \t", ImmutableMap.<SqlType, Object>builder()
            .put(INTEGER, laxOnly(10))
            .put(BIGINT, laxOnly(10L))
            .put(decimal(3, 0), laxOnly(new BigDecimal("10")))
            .put(DOUBLE, laxOnly(10.0D))
            .put(STRING, "\t 10 \t")
            .put(BYTES, ByteBuffer.wrap(new byte[] {(byte) 215}))
            .build()
        )
        .put("1.344e2", ImmutableMap.<SqlType, Object>builder()
            .put(decimal(4, 1), laxOnly(new BigDecimal("134.4")))
            .put(DOUBLE, laxOnly(134.4D))
            .put(STRING, "1.344e2")
            .build()
        )
        .put("10!", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "10!")
            .build()
        )
        .put("true!", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "true!")
            .build()
        )
        .put("NaN", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "NaN")
            .put(BYTES, ByteBuffer.wrap(new byte[] {53, (byte) 163}))
            .build()
        )
        .put("Infinity", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "Infinity")
            .put(BYTES, ByteBuffer.wrap(new byte[] {34, 119, (byte) 226, (byte) 158, 43, 114}))
            .build()
        )
        .put("2018-09-01T09:01:15.000", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "2018-09-01T09:01:15.000")
            .put(TIMESTAMP, new Timestamp(1535792475000L))
            .build()
        )
        .put("09:01:15", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "09:01:15")
            .put(TIME, new Time(32475000L))
            .build()
        )
        .put("2018-09-01", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "2018-09-01")
            .put(DATE, new Date(1535760000000L))
            .put(TIMESTAMP, new Timestamp(1535760000000L))
            .build()
        )
        .put("IQ==", ImmutableMap.<SqlType, Object>builder()
            .put(STRING, "IQ==")
            .put(BYTES, ByteBuffer.wrap(new byte[] {33}))
            .build()
        )
        // TIMESTAMP:
        .put(new Timestamp(1535792475000L), ImmutableMap.<SqlType, Object>builder()
            .put(TIMESTAMP, new Timestamp(1535792475000L))
            .put(STRING, laxOnly("2018-09-01T09:01:15.000"))
            .build()
        )
        .put(new Time(1000L), ImmutableMap.<SqlType, Object>builder()
            .put(TIME, new Time(1000L))
            .put(STRING, laxOnly("00:00:01"))
            .build()
        )
        .put(new Date(636451200000L), ImmutableMap.<SqlType, Object>builder()
            .put(DATE, new Date(636451200000L))
            .put(STRING, laxOnly("1990-03-03"))
            .build()
        )
        // BYTES:
        .put(ByteBuffer.wrap(new byte[] {110}), ImmutableMap.<SqlType, Object>builder()
            .put(BYTES, ByteBuffer.wrap(new byte[] {110}))
            .build()
        )
        // ARRAY:
        .put(ImmutableList.of(), ImmutableMap.<SqlType, Object>builder()
            .put(array(BOOLEAN), ImmutableList.of())
            .put(array(INTEGER), ImmutableList.of())
            .put(array(BIGINT), ImmutableList.of())
            .put(array(decimal(4, 2)), ImmutableList.of())
            .put(array(DOUBLE), ImmutableList.of())
            .put(array(STRING), ImmutableList.of())
            .put(array(array(BOOLEAN)), ImmutableList.of())
            .put(array(map(STRING, STRING)), ImmutableList.of())
            .put(array(SqlTypes.struct().field("a", INTEGER).build()), ImmutableList.of())
            .put(array(TIME), ImmutableList.of())
            .put(array(DATE), ImmutableList.of())
            .put(array(TIMESTAMP), ImmutableList.of())
            .put(array(BYTES), ImmutableList.of())
            .build()
        )
        .put(ImmutableList.of(true, false), ImmutableMap.<SqlType, Object>builder()
            .put(array(BOOLEAN), ImmutableList.of(true, false))
            .put(array(INTEGER), Result.failure())
            .put(array(STRING), laxOnly(ImmutableList.of("true", "false")))
            .build()
        )
        .put(ImmutableList.of(10L, -99L), ImmutableMap.<SqlType, Object>builder()
            .put(array(BOOLEAN), Result.failure())
            .put(array(BIGINT), ImmutableList.of(10L, -99L))
            .put(array(decimal(4, 2)), ImmutableList.of(
                new BigDecimal("10.00"), new BigDecimal("-99.00")
            ))
            .put(array(decimal(3, 2)), Result.failure())
            .put(array(DOUBLE), ImmutableList.of(10.0D, -99.0D))
            .put(array(STRING), laxOnly(ImmutableList.of("10", "-99")))
            .build()
        )
        // MAP:
        .put(ImmutableMap.of(), ImmutableMap.<SqlType, Object>builder()
            .put(map(BOOLEAN, BIGINT), ImmutableMap.of())
            .put(map(STRING, INTEGER), ImmutableMap.of())
            .put(map(DOUBLE, BIGINT), ImmutableMap.of())
            .put(map(STRING, decimal(4, 2)), ImmutableMap.of())
            .put(map(INTEGER, DOUBLE), ImmutableMap.of())
            .put(map(BOOLEAN, STRING), ImmutableMap.of())
            .put(map(INTEGER, TIMESTAMP), ImmutableMap.of())
            .put(map(array(BOOLEAN), array(STRING)), ImmutableMap.of())
            .put(map(map(STRING, STRING), map(STRING, BOOLEAN)), ImmutableMap.of())
            .put(map(
                SqlTypes.struct().field("a", INTEGER).build(),
                SqlTypes.struct().field("b", BIGINT).build()
            ), ImmutableMap.of())
            .build()
        )
        .put(ImmutableMap.of(true, false, false, true), ImmutableMap.<SqlType, Object>builder()
            .put(map(BOOLEAN, BOOLEAN), ImmutableMap.of(true, false, false, true))
            .put(map(STRING, BOOLEAN), laxOnly(ImmutableMap.of("true", false, "false", true)))
            .put(map(BOOLEAN, STRING), laxOnly(ImmutableMap.of(true, "false", false, "true")))
            .put(map(STRING, STRING), laxOnly(ImmutableMap.of("true", "false", "false", "true")))
            .put(map(INTEGER, STRING), Result.failure())
            .put(map(STRING, INTEGER), Result.failure())
            .build()
        )
        .put(ImmutableMap.of(10, new BigDecimal("123.450")), ImmutableMap.<SqlType, Object>builder()
            .put(map(INTEGER, decimal(6, 3)), ImmutableMap.of(10, new BigDecimal("123.450")))
            .put(map(BIGINT, decimal(18, 4)), ImmutableMap.of(10L, new BigDecimal("123.4500")))
            .put(map(INTEGER, decimal(6, 1)), Result.failure())
            .put(map(DOUBLE, DOUBLE), ImmutableMap.of(10.0D, 123.45D))
            .put(map(STRING, STRING), laxOnly(ImmutableMap.of("10", "123.450")))
            .build()
        )
        // STRUCT:
        .put(createStruct(SqlTypes.struct().field("a", INTEGER).build()),
            ImmutableMap.<SqlType, Object>builder()
                .put(
                    struct().build(),
                    createStruct(SqlTypes.struct().build())
                )
                .put(
                    struct().field("a", BOOLEAN).build(),
                    createStruct(struct().field("a", BOOLEAN).build())
                )
                .put(
                    struct().field("a", INTEGER).build(),
                    createStruct(struct().field("a", INTEGER).build())
                )
                .put(
                    struct().field("a", DOUBLE).build(),
                    createStruct(struct().field("a", DOUBLE).build())
                )
                .put(
                    struct().field("a", STRING).build(),
                    createStruct(struct().field("a", STRING).build())
                )
                .put(
                    struct().field("a", map(STRING, BOOLEAN)).build(),
                    createStruct(struct().field("a", map(STRING, BOOLEAN)).build())
                )
                .put(
                    struct().field("a", array(STRING)).build(),
                    createStruct(struct().field("a", array(STRING)).build())
                )
                .put(struct().field("a", struct().field("a", INTEGER).build()).build(),
                    createStruct(struct().field("a", struct().field("a", INTEGER).build()).build())
                )
                .put(
                    struct().field("b", BOOLEAN).build(),
                    createStruct(struct().field("b", BOOLEAN).build())
                )
                .put(
                    struct().field("a", TIMESTAMP).build(),
                    createStruct(struct().field("a", TIMESTAMP).build())
                )
                .put(
                    struct().field("abc", TIME).build(),
                    createStruct(struct().field("abc", TIME).build())
                )
                .put(
                    struct().field("def", DATE).build(),
                    createStruct(struct().field("def", DATE).build())
                )
                .put(
                    struct().field("ghi", BYTES).build(),
                    createStruct(struct().field("ghi", BYTES).build())
                )
                .build()
        )
        .put(createStruct(SqlTypes.struct()
                .field("a", BOOLEAN)
                .field("b", STRING)
                .field("c", INTEGER)
                .field("d", STRING)
                .build())
                .put("a", true)
                .put("b", "false")
                .put("c", 9)
                .put("d", "11.0"),
            ImmutableMap.<SqlType, Object>builder()
                .put(
                    struct().field("a", BOOLEAN).build(),
                    createStruct(struct().field("a", BOOLEAN).build())
                        .put("a", true)
                )
                .put(
                    struct().field("b", BOOLEAN).build(),
                    laxOnly(createStruct(struct().field("b", BOOLEAN).build())
                        .put("b", false))
                )
                .put(
                    struct().field("c", DOUBLE).build(),
                    createStruct(struct().field("c", DOUBLE).build())
                        .put("c", 9.0D)
                )
                .put(
                    struct().field("d", DOUBLE).build(),
                    laxOnly(createStruct(struct().field("d", DOUBLE).build())
                        .put("d", 11.0D))
                )
                .put(
                    struct().field("b", INTEGER).build(),
                    Result.failure()
                )
                .put(
                    struct().field("e", INTEGER).build(),
                    createStruct(struct().field("e", INTEGER).build())
                )
                .put(
                    struct().field("a", INTEGER).build(),
                    Result.failure()
                )
                .build()
        )
        .build();

    private final DefaultSqlValueCoercer coercer;
    private final Object value;
    private final SqlType to;
    private final Result expected;

    @Parameterized.Parameters(name = "{0} coerced to {1} should be {2}")
    public static Collection<Object[]> testCases() {
      return TEST_CASES.entrySet().stream()
          .flatMap(LaxValueCoercionTest::buildTestCases)
          .collect(Collectors.toList());
    }

    @SuppressWarnings("unused") // Invoked via reflection by Junit.
    public LaxValueCoercionTest(final Object value, final SqlType to, final Object expected) {
      this(DefaultSqlValueCoercer.LAX, value, to, expected);
    }

    LaxValueCoercionTest(
        final DefaultSqlValueCoercer coercer,
        final Object value,
        final SqlType to,
        final Object expected
    ) {
      this.coercer = requireNonNull(coercer, "coercer");
      this.value = requireNonNull(value, "value");
      this.to = requireNonNull(to, "to");
      this.expected = LaxOnly.unwrap(expected);
    }

    @Test
    public void shouldCoerce() {
      // When:
      final Result result = coercer.coerce(value, to);

      // Then:
      assertThat(result, is(expected));
    }

    private static Stream<Object[]> buildTestCases(final Entry<Object, Map<SqlType, Object>> x) {
      final Object value = x.getKey();
      final Map<SqlType, Object> positiveExpectedResponses = x.getValue();

      final Stream<Object[]> coveredCases = positiveExpectedResponses.entrySet().stream()
          .map(y -> new Object[]{value, y.getKey(), y.getValue()});

      final Set<SqlBaseType> basesCovered = positiveExpectedResponses.keySet().stream()
          .map(SqlType::baseType)
          .collect(Collectors.toSet());

      final Set<SqlBaseType> basesNotCovered = Sets
          .difference(ImmutableSet.copyOf(SqlBaseType.values()), basesCovered);

      final Stream<Object[]> implicitlyUnsupported = basesNotCovered.stream()
          .map(base -> new Object[]{
              value,
              TypeInstances.typeInstanceFor(base),
              Result.failure()
          });

      return Streams.concat(coveredCases, implicitlyUnsupported);
    }

    private static Struct createStruct(final SqlStruct type) {
      final Schema schema = SchemaConverters.sqlToConnectConverter().toConnectSchema(type);
      return new Struct(schema);
    }

    private static LaxOnly laxOnly(final Object expected) {
      return new LaxOnly(expected);
    }

    /**
     * Marks the expected result of a test case to only be value for the lax coercer. The strict
     * coercer should return a failure.
     */
    public static final class LaxOnly {

      private final Object expected;

      LaxOnly(final Object expected) {
        this.expected = requireNonNull(expected, "expected");
      }

      public static Result unwrap(final Object expected) {
        if (expected instanceof Result) {
          return (Result) expected;
        }

        if (expected instanceof LaxOnly) {
          return Result.of(((LaxOnly) expected).expected);
        }

        return Result.of(expected);
      }

      @Override
      public String toString() {
        return expected.toString();
      }
    }
  }

  public static final class StrictValueCoercionTest extends LaxValueCoercionTest {

    public StrictValueCoercionTest(final Object value, final SqlType to, final Object expected) {
      super(
          DefaultSqlValueCoercer.STRICT,
          value,
          to,
          expected instanceof LaxOnly ? Result.failure() : LaxOnly.unwrap(expected)
      );
    }
  }

  // Tests all combinations of types
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @RunWith(Parameterized.class)
  public static class TypeCombinationTest {

    @Parameterized.Parameters(name = "{0}: {1} -> {2}")
    public static Collection<Object[]> testCases() {
      return Arrays.stream(DefaultSqlValueCoercer.values())
          .flatMap(coercer -> Arrays.stream(SqlBaseType.values())
              .flatMap(from -> Arrays.stream(SqlBaseType.values())
                  .map(to -> new Object[]{coercer, from, to}))
          )
          .collect(Collectors.toList());
    }

    private final DefaultSqlValueCoercer coercer;
    private final SqlType from;
    private final SqlType to;
    private final Class<?> returnJavaType;
    private final boolean supportedCoercion;

    @SuppressWarnings("unused") // Invoked by Parameterized
    public TypeCombinationTest(
        final DefaultSqlValueCoercer coercer,
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      this(
          coercer,
          TypeInstances.typeInstanceFor(from),
          TypeInstances.typeInstanceFor(to),
          SupportedCoercions.supported(coercer, from, to)
      );
    }

    TypeCombinationTest(
        final DefaultSqlValueCoercer coercer,
        final SqlType from,
        final SqlType to,
        final boolean supportedCoercion
    ) {
      this.coercer = requireNonNull(coercer, "coercer");
      this.from = requireNonNull(from, "from");
      this.to = requireNonNull(to, "to");
      this.returnJavaType = SchemaConverters.sqlToJavaConverter().toJavaType(to);
      this.supportedCoercion = supportedCoercion;
    }

    @Test
    public void shouldThrowOnUnsupported() {
      if (!supportedCoercion) {
        assertUnsupported(coercer, from, to);
      }
    }

    @Test
    public void shouldEvalCodeWithNonNullArgument() {
      if (supportedCoercion) {
        // Given:
        final Object argument = InstanceInstances.instanceFor(from, to);

        // When:
        final Optional<?> result = coerce(coercer, from, to, argument);

        // Then:
        assertThat("return type mismatch", result.get(), instanceOf(returnJavaType));
      }
    }

    @Test
    public void shouldEvalCodeWithNullArgument() {
      if (supportedCoercion) {
        // When:
        final Optional<?> result = coerce(coercer, from, to, null);

        // Then:
        assertThat("null should return null", result, is(Optional.empty()));
      }
    }
  }

  // Tests all combinations of array element types
  public static final class ArrayCombinationTest extends TypeCombinationTest {

    public ArrayCombinationTest(
        final DefaultSqlValueCoercer coercer,
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      super(
          coercer,
          array(TypeInstances.typeInstanceFor(from)),
          array(TypeInstances.typeInstanceFor(to)),
          SupportedCoercions.supported(coercer, from, to)
      );
    }
  }

  public static final class ArrayAdditionalTest {

    protected static final DefaultSqlValueCoercer COERCER = DefaultSqlValueCoercer.STRICT;

    @Test
    public void shouldMapElements() {
      // Given:
      final SqlType from = array(BIGINT);
      final SqlType to = array(DOUBLE);
      final List<Long> argument = Arrays.asList(1L, 2L, 3L);

      // When:
      final Optional<?> result = coerce(COERCER, from, to, argument);

      // Then:
      assertThat(result, is(Optional.of(Arrays.asList(1.0D, 2.0D, 3.0D))));
    }

    @Test
    public void shouldHandleNulls() {
      // Given:
      final SqlType from = array(BIGINT);
      final SqlType to = array(DOUBLE);
      final List<Long> argument = Arrays.asList(null, null);

      // When:
      final Optional<?> result = coerce(COERCER, from, to, argument);

      // Then:
      assertThat(result, is(Optional.of(Arrays.asList(null, null))));
    }
  }

  // Tests all combinations of map key types
  public static final class MapKeyCombinationTest extends TypeCombinationTest {

    public MapKeyCombinationTest(
        final DefaultSqlValueCoercer coercer,
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      super(
          coercer,
          map(TypeInstances.typeInstanceFor(from), INTEGER),
          map(TypeInstances.typeInstanceFor(to), INTEGER),
          SupportedCoercions.supported(coercer, from, to)
      );
    }
  }

  // Tests all combinations of map value types
  public static final class MapValueCombinationTest extends TypeCombinationTest {

    public MapValueCombinationTest(
        final DefaultSqlValueCoercer coercer,
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      super(
          coercer,
          map(INTEGER, TypeInstances.typeInstanceFor(from)),
          map(INTEGER, TypeInstances.typeInstanceFor(to)),
          SupportedCoercions.supported(coercer, from, to)
      );
    }
  }

  public static final class MapAdditionalTest {

    protected static final DefaultSqlValueCoercer COERCER = DefaultSqlValueCoercer.STRICT;

    @Test
    public void shouldMapElements() {
      // Given:
      final SqlType from = map(INTEGER, BIGINT);
      final SqlType to = map(BIGINT, DOUBLE);
      final Map<Integer, Long> argument = ImmutableMap.of(1, 2L, 3, 4L);

      // When:
      final Optional<?> result = coerce(COERCER, from, to, argument);

      // Then:
      assertThat(result, is(Optional.of(ImmutableMap.of(1L, 2.0D, 3L, 4.0D))));
    }

    @Test
    public void shouldHandleNulls() {
      // Given:
      final SqlType from = map(INTEGER, BIGINT);
      final SqlType to = map(BIGINT, DOUBLE);
      final Map<Integer, Long> argument = new HashMap<>();
      argument.put(1, null);
      argument.put(null, 2L);

      // When:
      final Optional<?> result = coerce(COERCER, from, to, argument);

      // Then:
      final Map<Long, Double> expected = new HashMap<>();
      expected.put(1L, null);
      expected.put(null, 2.0D);

      assertThat(result, is(Optional.of(expected)));
    }
  }

  // Tests all combinations of struct field types
  public static final class StructCombinationTest extends TypeCombinationTest {

    public StructCombinationTest(
        final DefaultSqlValueCoercer coercer,
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      super(
          coercer,
          SqlTypes.struct().field("a", TypeInstances.typeInstanceFor(from)).build(),
          SqlTypes.struct().field("a", TypeInstances.typeInstanceFor(to)).build(),
          SupportedCoercions.supported(coercer, from, to)
      );
    }
  }

  public static final class StructAdditionalTest {

    private final DefaultSqlValueCoercer coercer = DefaultSqlValueCoercer.STRICT;

    @Test
    public void shouldMapFields() {
      // Given:
      final SqlStruct from = SqlTypes.struct()
          .field("a", INTEGER)
          .field("b", BIGINT)
          .build();

      final SqlStruct to = SqlTypes.struct()
          .field("a", BIGINT)
          .field("b", DOUBLE)
          .build();

      final Object value = InstanceInstances.instanceFor(from, to);

      // When:
      final Result result = coercer.coerce(value, to);

      // Then:
      assertThat(result, is(Result.of(
          new Struct(SchemaConverters.sqlToConnectConverter().toConnectSchema(to))
              .put("a", 10L)
              .put("b", 99.0D)
      )));
    }

    @Test
    public void shouldHandleNullFieldValues() {
      // Given:
      // Given:
      final SqlStruct from = SqlTypes.struct()
          .field("a", INTEGER)
          .field("b", INTEGER)
          .build();

      final SqlStruct to = SqlTypes.struct()
          .field("a", BIGINT)
          .field("b", DOUBLE)
          .build();

      final Struct value = (Struct) InstanceInstances.instanceFor(from, to);
      value.put("a", null);
      value.put("b", null);

      // When:
      final Result result = coercer.coerce(value, to);

      // Then:
      assertThat(result, is(Result.of(
          new Struct(SchemaConverters.sqlToConnectConverter().toConnectSchema(to))
              .put("a", null)
              .put("b", null)
      )));
    }

    @Test
    public void shouldReturnWidenedStructFromCanCoerce() {
      // Given:
      final SqlStruct from = SqlTypes.struct()
          .field("onlyFrom", INTEGER)
          .field("common", INTEGER)
          .build();

      final SqlStruct to = SqlTypes.struct()
          .field("onlyTo", INTEGER)
          .field("common", INTEGER)
          .build();

      // When:
      final Optional<SqlType> sqlType = coercer.canCoerce(from, to);

      // Then:
      assertThat(sqlType, is(Optional.of(SqlTypes.struct()
          .field("onlyFrom", INTEGER)
          .field("common", INTEGER)
          .field("onlyTo", INTEGER)
          .build()
      )));
    }

    @Test
    public void shouldHandlingFieldsMissingFromTo() {
      // Given:
      final SqlStruct from = SqlTypes.struct()
          .field("onlyFrom", INTEGER)
          .field("common", INTEGER)
          .build();

      final SqlStruct to = SqlTypes.struct()
          .field("common", INTEGER)
          .build();

      final Object value = InstanceInstances.instanceFor(from, to);

      // When:
      final Result result = coercer.coerce(value, to);

      // Then:
      assertThat(result, is(Result.of(
          new Struct(SchemaConverters.sqlToConnectConverter().toConnectSchema(to))
              .put("common", 10)
      )));
    }

    @Test
    public void shouldHandlingAdditionalFieldsInTo() {
      // Given:
      final SqlStruct from = SqlTypes.struct()
          .field("common", INTEGER)
          .build();

      final SqlStruct to = SqlTypes.struct()
          .field("onlyTo", INTEGER)
          .field("common", INTEGER)
          .build();

      final Object value = InstanceInstances.instanceFor(from, to);

      // When:
      final Result result = coercer.coerce(value, to);

      // Then:
      assertThat(result, is(Result.of(
          new Struct(SchemaConverters.sqlToConnectConverter().toConnectSchema(to))
              .put("onlyTo", null)
              .put("common", 10)
      )));
    }
  }

  @RunWith(Parameterized.class)
  public static final class UpCastRulesTest {

    @Parameterized.Parameters(name = "{0}: {1} -> {2}")
    public static Collection<Object[]> testCases() {
      return Arrays.stream(SqlBaseType.values())
          .flatMap(from -> Arrays.stream(SqlBaseType.values())
              .map(to -> new Object[]{from, to}))
          .collect(Collectors.toList());
    }

    private final DefaultSqlValueCoercer coercer = DefaultSqlValueCoercer.STRICT;
    private final SqlBaseType from;
    private final SqlBaseType to;

    public UpCastRulesTest(
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      this.from = requireNonNull(from, "from");
      this.to = requireNonNull(to, "to");
    }

    @Test
    public void shouldCoerceUsingSameRulesAsBaseTypeUpCastRules() {
      // Given:
      final SqlType fromType = TypeInstances.typeInstanceFor(from);
      final SqlType toType = TypeInstances.typeInstanceFor(to);
      final Object value = InstanceInstances.instanceFor(fromType, toType);

      // Then:
      if (coercionShouldBeSupported(from, to)) {
        assertThat(
            "should coerce " + from + " to " + to,
            coercer.coerce(value, toType),
            is(not(Result.failure()))
        );
      } else {
        assertThat(
            "should not coerce " + from + " to " + to,
            coercer.coerce(value, toType),
            is(Result.failure())
        );
      }
    }

    private static boolean coercionShouldBeSupported(
        final SqlBaseType fromBaseType,
        final SqlBaseType toBaseType
    ) {
      return fromBaseType.canImplicitlyCast(toBaseType);
    }
  }

  public static final class MetaTest {

    @Test
    public void shouldFailIfNewSqlBaseTypeAdded() {
      final Set<SqlBaseType> allTypes = Arrays.stream(SqlBaseType.values())
          .collect(Collectors.toSet());

      assertThat(
          "This test will fail is a new base type is added to remind you to think about what"
              + "coercions should be supported for the new type.",
          allTypes,
          is(ImmutableSet.of(
              SqlBaseType.BOOLEAN, SqlBaseType.INTEGER, SqlBaseType.BIGINT, SqlBaseType.DECIMAL,
              SqlBaseType.DOUBLE, SqlBaseType.STRING, SqlBaseType.ARRAY, SqlBaseType.MAP,
              SqlBaseType.STRUCT, SqlBaseType.TIME, SqlBaseType.DATE, SqlBaseType.TIMESTAMP,
              SqlBaseType.BYTES
          ))
      );
    }

    @Test
    public void shouldCoverAllBaseTypsInValueCoercionTest() {
      final Set<SqlBaseType> basesCovered = LaxValueCoercionTest.TEST_CASES.keySet().stream()
          .map(v -> SchemaConverters.javaToSqlConverter().toSqlType(v.getClass()))
          .collect(Collectors.toSet());

      final Set<SqlBaseType> notCovered = Sets
          .difference(ImmutableSet.copyOf(SqlBaseType.values()), basesCovered);

      assertThat(
          "This test will fail if a new base type is added and no corresponding test cases "
              + "are added to ValueCoercionTest.TEST_CASES. Please add appropriate test cases.",
          notCovered,
          is(empty())
      );
    }
  }

  private static void assertUnsupported(
      final DefaultSqlValueCoercer coercer,
      final SqlType from,
      final SqlType to
  ) {
    // Given:
    final Object value = InstanceInstances.instanceFor(from, to);

    // When:
    final Optional<SqlType> coercedType = coercer.canCoerce(from, to);
    final Result result = coercer.coerce(value, to);

    // Then:
    assertThat("canCoerce(" + from + "," + to + ")", coercedType, is(Optional.empty()));
    assertThat("coerce(" + value + "," + to + ")", result, is(Result.failure()));
  }

  private static Optional<?> coerce(
      final DefaultSqlValueCoercer coercer,
      final SqlType from,
      final SqlType to,
      final Object value
  ) {
    // When:
    final Optional<SqlType> coercedType = coercer.canCoerce(from, to);
    final Result result = coercer.coerce(value, to);

    // Then:
    assertThat("canCoerce(" + from + "," + to + ")", coercedType, is(not(Optional.empty())));
    assertThat("coerce(" + value + "," + to + ")", result, is(not(Result.failure())));
    return result.value();
  }

  private static final class TypeInstances {

    private static final ImmutableMap<SqlBaseType, SqlType> TYPE_INSTANCES = ImmutableMap.
        <SqlBaseType, SqlType>builder()
        .put(SqlBaseType.BOOLEAN, SqlTypes.BOOLEAN)
        .put(SqlBaseType.INTEGER, SqlTypes.INTEGER)
        .put(SqlBaseType.BIGINT, SqlTypes.BIGINT)
        .put(SqlBaseType.DECIMAL, decimal(4, 2))
        .put(SqlBaseType.DOUBLE, SqlTypes.DOUBLE)
        .put(SqlBaseType.STRING, SqlTypes.STRING)
        .put(SqlBaseType.ARRAY, array(SqlTypes.BIGINT))
        .put(SqlBaseType.MAP, map(SqlTypes.BIGINT, SqlTypes.STRING))
        .put(SqlBaseType.STRUCT, SqlTypes.struct()
            .field("Bob", SqlTypes.STRING)
            .build())
        .put(SqlBaseType.TIME, SqlTypes.TIME)
        .put(SqlBaseType.DATE, SqlTypes.DATE)
        .put(SqlBaseType.TIMESTAMP, SqlTypes.TIMESTAMP)
        .put(SqlBaseType.BYTES, BYTES)
        .build();

    static SqlType typeInstanceFor(final SqlBaseType baseType) {
      final SqlType sqlType = TYPE_INSTANCES.get(baseType);
      assertThat(
          "Invalid test: missing type instance for " + baseType,
          sqlType,
          is(notNullValue())
      );
      return sqlType;
    }
  }

  private static final class InstanceInstances {

    private static final ImmutableMap<SqlBaseType, Object> INSTANCES = ImmutableMap.
        <SqlBaseType, Object>builder()
        .put(SqlBaseType.BOOLEAN, true)
        .put(SqlBaseType.INTEGER, 10)
        .put(SqlBaseType.BIGINT, 99L)
        .put(SqlBaseType.DECIMAL, new BigDecimal("12.01"))
        .put(SqlBaseType.DOUBLE, 34.98d)
        .put(SqlBaseType.STRING, "11")
        .put(SqlBaseType.TIME, new Time(1000L))
        .put(SqlBaseType.DATE, new Date(636451200000L))
        .put(SqlBaseType.TIMESTAMP, new Timestamp(1535792475000L))
        .put(SqlBaseType.BYTES, ByteBuffer.wrap(new byte[] {88, 34, 120}))
        .build();

    @SuppressWarnings("fallthrough")
    static Object instanceFor(final SqlType from, final SqlType to) {
      switch (from.baseType()) {
        case ARRAY:
          final SqlArray sqlArray = (SqlArray) from;
          final SqlType elementTo = to instanceof SqlArray ? ((SqlArray) to).getItemType() : to;
          final Object element = instanceFor(sqlArray.getItemType(), elementTo);
          return ImmutableList.of(element);
        case MAP:
          final SqlMap sqlMap = (SqlMap) from;
          final SqlType keyTo = to instanceof SqlMap ? ((SqlMap) to).getKeyType() : to;
          final SqlType valueTo = to instanceof SqlMap ? ((SqlMap) to).getValueType() : to;
          final Object key = instanceFor(sqlMap.getKeyType(), keyTo);
          final Object value = instanceFor(sqlMap.getValueType(), valueTo);
          return ImmutableMap.of(key, value);
        case STRUCT:
          final SqlStruct sqlStruct = (SqlStruct) from;
          final Struct struct =
              new Struct(SchemaConverters.sqlToConnectConverter().toConnectSchema(from));

          sqlStruct.fields().forEach(field -> {
            final SqlType fieldTo = to instanceof SqlStruct
                ? ((SqlStruct) to).field(field.name())
                .map(Field::type)
                .orElse(to)
                : to;
            struct.put(field.name(), instanceFor(field.type(), fieldTo));
          });

          return struct;
        case STRING:
          if (to.baseType() == SqlBaseType.BOOLEAN) {
            return "true";
          }
          if (to.baseType() == SqlBaseType.TIMESTAMP) {
            return "2018-09-01T09:01:15.000";
          }
          if (to.baseType() == SqlBaseType.TIME) {
            return "09:01:15";
          }
          if (to.baseType() == SqlBaseType.DATE) {
            return "2018-09-01";
          }
          // Intentional fall through
        default:
          final Object instance = INSTANCES.get(from.baseType());
          assertThat(
              "Invalid test: missing instance for " + from.baseType(),
              instance,
              is(notNullValue())
          );
          return instance;
      }
    }
  }

  private static final class SupportedCoercions {

    private static final ImmutableMap<SqlBaseType, ImmutableSet<SqlBaseType>> STRICT_SUPPORTED =
        ImmutableMap.<SqlBaseType, ImmutableSet<SqlBaseType>>builder()
            .put(SqlBaseType.BOOLEAN, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BOOLEAN)
                .build())
            .put(SqlBaseType.INTEGER, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .build())
            .put(SqlBaseType.BIGINT, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .build())
            .put(SqlBaseType.DECIMAL, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .build())
            .put(SqlBaseType.DOUBLE, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.DOUBLE)
                .build())
            .put(SqlBaseType.STRING, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .add(SqlBaseType.TIME)
                .add(SqlBaseType.DATE)
                .add(SqlBaseType.TIMESTAMP)
                .add(SqlBaseType.BYTES)
                .build())
            .put(SqlBaseType.ARRAY, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.ARRAY)
                .build())
            .put(SqlBaseType.MAP, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.MAP)
                .build())
            .put(SqlBaseType.STRUCT, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRUCT)
                .build())
            .put(SqlBaseType.TIME, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.TIME)
                .build())
            .put(SqlBaseType.DATE, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.DATE)
                .build())
            .put(SqlBaseType.TIMESTAMP, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.TIMESTAMP)
                .build())
            .put(SqlBaseType.BYTES, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BYTES)
                .build())
            .build();

    private static final ImmutableMap<SqlBaseType, ImmutableSet<SqlBaseType>> LAX_ADDITIONAL =
        ImmutableMap.<SqlBaseType, ImmutableSet<SqlBaseType>>builder()
            .put(SqlBaseType.BOOLEAN, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build()
            )
            .put(SqlBaseType.INTEGER, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build()
            )
            .put(SqlBaseType.BIGINT, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build()
            )
            .put(SqlBaseType.DECIMAL, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build()
            )
            .put(SqlBaseType.DOUBLE, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build()
            )
            .put(SqlBaseType.STRING, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BOOLEAN)
                .add(SqlBaseType.INTEGER)
                .add(SqlBaseType.BIGINT)
                .add(SqlBaseType.DECIMAL)
                .add(SqlBaseType.DOUBLE)
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.TIMESTAMP, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.TIME, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.DATE, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.STRING)
                .build())
            .put(SqlBaseType.BYTES, ImmutableSet.<SqlBaseType>builder()
                .add(SqlBaseType.BYTES)
                .build())
            .build();

    private static boolean supported(
        final DefaultSqlValueCoercer coercer,
        final SqlBaseType from,
        final SqlBaseType to
    ) {
      if (coercer == DefaultSqlValueCoercer.LAX) {
        final ImmutableSet<SqlBaseType> sqlBaseTypes = LAX_ADDITIONAL
            .getOrDefault(from, ImmutableSet.of());
        // needed to avoid spotbugs warning with guava 32.0.1
        if (sqlBaseTypes == null) {
          return false;
        }
        final boolean supported = sqlBaseTypes.contains(to);
        if (supported) {
          return true;
        }
      }

      final ImmutableSet<SqlBaseType> supportedReturnTypes = STRICT_SUPPORTED.get(from);
      assertThat(
          "Invalid Test: missing expected result for: " + from,
          supportedReturnTypes, is(notNullValue())
      );

      return supportedReturnTypes.contains(to);
    }
  }
}