/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.codegen.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;


@RunWith(Enclosed.class)
public class CastEvaluatorTest {

  private static final String INNER_CODE = "some java code";

  private static final String TO_STRING_CODE = "String.valueOf(%s)";

  @RunWith(MockitoJUnitRunner.class)
  public static class ConfigTests {

    @Mock
    private KsqlConfig ksqlConfig;

    @Test
    public void shouldCastToStringUsingObjectsToString() {
      // Given:
      when(ksqlConfig.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE))
          .thenReturn(true);

      assertCast(
          SqlTypes.DOUBLE,
          SqlTypes.STRING,
          "Objects.toString(%s, null)",
          ksqlConfig
      );
    }

    @Test
    public void shouldCastToStringUsingStringValueOf() {
      // Given:
      when(ksqlConfig.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE))
          .thenReturn(false);

      // Then:
      assertCast(
          SqlTypes.DOUBLE,
          SqlTypes.STRING,
          "String.valueOf(%s)",
          ksqlConfig
      );
    }
  }

  @RunWith(Parameterized.class)
  public static class CombinationTest {

    private static final String UNSUPPORTED_CAST = "<UNSUPPORTED>";

    private static final ImmutableMap<SqlBaseType, ImmutableMap<SqlBaseType, String>> CODE =
        ImmutableMap.<SqlBaseType, ImmutableMap<SqlBaseType, String>>builder()
            .put(SqlBaseType.BOOLEAN, expectedBuilder()
                .put(SqlBaseType.BOOLEAN, INNER_CODE)
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .put(SqlBaseType.INTEGER, expectedBuilder()
                .put(SqlBaseType.INTEGER, INNER_CODE)
                .put(SqlBaseType.BIGINT, "(new Integer(%s).longValue())")
                .put(SqlBaseType.DECIMAL, "(DecimalUtil.cast(%s, 4, 2))")
                .put(SqlBaseType.DOUBLE, "(new Integer(%s).doubleValue())")
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .put(SqlBaseType.BIGINT, expectedBuilder()
                .put(SqlBaseType.INTEGER, "(new Long(%s).intValue())")
                .put(SqlBaseType.BIGINT, INNER_CODE)
                .put(SqlBaseType.DECIMAL, "(DecimalUtil.cast(%s, 4, 2))")
                .put(SqlBaseType.DOUBLE, "(new Long(%s).doubleValue())")
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .put(SqlBaseType.DECIMAL, expectedBuilder()
                .put(SqlBaseType.INTEGER, "((%s).intValue())")
                .put(SqlBaseType.BIGINT, "((%s).longValue())")
                .put(SqlBaseType.DECIMAL, INNER_CODE)
                .put(SqlBaseType.DOUBLE, "((%s).doubleValue())")
                .put(SqlBaseType.STRING, "%s.toPlainString()")
                .build())
            .put(SqlBaseType.DOUBLE, expectedBuilder()
                .put(SqlBaseType.INTEGER, "(new Double(%s).intValue())")
                .put(SqlBaseType.BIGINT, "(new Double(%s).longValue())")
                .put(SqlBaseType.DECIMAL, "(DecimalUtil.cast(%s, 4, 2))")
                .put(SqlBaseType.DOUBLE, INNER_CODE)
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .put(SqlBaseType.STRING, expectedBuilder()
                .put(SqlBaseType.BOOLEAN, "Boolean.parseBoolean(%s)")
                .put(SqlBaseType.INTEGER, "Integer.parseInt(%s)")
                .put(SqlBaseType.BIGINT, "Long.parseLong(%s)")
                .put(SqlBaseType.DECIMAL, "(DecimalUtil.cast(%s, 4, 2))")
                .put(SqlBaseType.DOUBLE, "Double.parseDouble(%s)")
                .put(SqlBaseType.STRING, INNER_CODE)
                .build())
            .put(SqlBaseType.ARRAY, expectedBuilder()
                .put(SqlBaseType.ARRAY, INNER_CODE)
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .put(SqlBaseType.MAP, expectedBuilder()
                .put(SqlBaseType.MAP, INNER_CODE)
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .put(SqlBaseType.STRUCT, expectedBuilder()
                .put(SqlBaseType.STRUCT, INNER_CODE)
                .put(SqlBaseType.STRING, TO_STRING_CODE)
                .build())
            .build();

    @Parameterized.Parameters(name = "{0} -> {1}")
    public static Collection<SqlBaseType[]> getMethodsToTest() {

      return Arrays.stream(SqlBaseType.values())
          .flatMap(sourceType -> Arrays.stream(SqlBaseType.values())
              .map(targetType -> new SqlBaseType[]{sourceType, targetType}))
          .collect(Collectors.toList());
    }

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private KsqlConfig ksqlConfig;

    private final SqlType sourceType;
    private final SqlType returnType;
    private final String expected;

    public CombinationTest(final SqlBaseType sourceType, final SqlBaseType returnType) {
      this.sourceType = TypeInstances.typeInstanceFor(sourceType);
      this.returnType = TypeInstances.typeInstanceFor(returnType);
      this.expected = expected(sourceType, returnType);
    }

    @Test
    public void shouldHandleCombination() {
      if (expected.equals(UNSUPPORTED_CAST)) {
        assertUnsupported(sourceType, returnType, ksqlConfig);
      } else {
        assertCast(sourceType, returnType, expected, ksqlConfig);
      }
    }

    private static String expected(final SqlBaseType sourceType, final SqlBaseType returnType) {
      final ImmutableMap<SqlBaseType, String> byReturnType = CODE.get(sourceType);
      assertThat(
          "Invalid Test: missing expected result for: " + sourceType,
          byReturnType, is(notNullValue())
      );

      final String expected = byReturnType.get(returnType);

      assertThat(
          "Invalid Test: missing expected result for : " + sourceType + "->" + returnType,
          expected, is(notNullValue())
      );

      return expected;
    }

    private static ExpectedBuilder expectedBuilder() {
      return new ExpectedBuilder();
    }

    private static final class ExpectedBuilder {

      private final ImmutableMap.Builder<SqlBaseType, String> cases = ImmutableMap.builder();

      public ExpectedBuilder put(final SqlBaseType baseType, final String expected) {
        cases.put(baseType, expected);
        return this;
      }

      public ImmutableMap<SqlBaseType, String> build() {
        putUnsupported();
        return cases.build();
      }

      private void putUnsupported() {
        final Set<SqlBaseType> seen = cases.build().keySet();
        Arrays.stream(SqlBaseType.values())
            .filter(type -> !seen.contains(type))
            .forEach(type -> cases.put(type, UNSUPPORTED_CAST));
      }
    }
  }

  private static void assertCast(
      final SqlType sourceType,
      final SqlType returnType,
      final String expectedCode,
      final KsqlConfig ksqlConfig
  ) {
    // When:
    final Pair<String, SqlType> result = CastEvaluator
        .eval(Pair.of(INNER_CODE, sourceType), returnType, ksqlConfig);

    // Then:
    assertThat(result.getLeft(), is(formatCode(expectedCode)));
    assertThat(result.getRight(), is(returnType));
  }

  private static void assertUnsupported(
      final SqlType sourceType,
      final SqlType returnType,
      final KsqlConfig ksqlConfig
  ) {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CastEvaluator.eval(Pair.of(INNER_CODE, sourceType), returnType, ksqlConfig)
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("Cast of " + sourceType + " to " + returnType + " is not supported")
    );
  }

  private static String formatCode(final String code) {
    return String.format(code, INNER_CODE);
  }

  private static final class TypeInstances {

    private static final ImmutableMap<SqlBaseType, SqlType> TYPE_INSTANCES = ImmutableMap.
        <SqlBaseType, SqlType>builder()
        .put(SqlBaseType.BOOLEAN, SqlTypes.BOOLEAN)
        .put(SqlBaseType.INTEGER, SqlTypes.INTEGER)
        .put(SqlBaseType.BIGINT, SqlTypes.BIGINT)
        .put(SqlBaseType.DECIMAL, SqlTypes.decimal(4, 2))
        .put(SqlBaseType.DOUBLE, SqlTypes.DOUBLE)
        .put(SqlBaseType.STRING, SqlTypes.STRING)
        .put(SqlBaseType.ARRAY, SqlTypes.array(SqlTypes.BIGINT))
        .put(SqlBaseType.MAP, SqlTypes.map(SqlTypes.BIGINT, SqlTypes.STRING))
        .put(SqlBaseType.STRUCT, SqlTypes.struct()
            .field("Bob", SqlTypes.STRING)
            .build())
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
}