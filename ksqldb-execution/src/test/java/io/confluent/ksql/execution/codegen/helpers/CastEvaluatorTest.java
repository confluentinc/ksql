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
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.Pair;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CastEvaluatorTest {

  private static final String INNER_CODE = "some java code";

  @Mock
  private KsqlConfig ksqlConfig;

  @Test
  public void shouldCastBigIntToInt() {
    assertCast(
        SqlTypes.BIGINT,
        SqlTypes.INTEGER,
        is(formatCode("(new Long(%s).intValue())"))
    );
  }

  @Test
  public void shouldCastDoubleToBigInt() {
    assertCast(
        SqlTypes.DOUBLE,
        SqlTypes.BIGINT,
        is(formatCode("(new Double(%s).longValue())"))
    );
  }

  @Test
  public void shouldCastDoubleToStringUsingObjectsToString() {
    // Given:
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE))
        .thenReturn(true);

    assertCast(
        SqlTypes.DOUBLE,
        SqlTypes.STRING,
        is(formatCode("Objects.toString(%s, null)"))
    );
  }

  @Test
  public void shouldCastDoubleToStringUsingStringValueOf() {
    // Given:
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE))
        .thenReturn(false);

    // Then:
    assertCast(
        SqlTypes.DOUBLE,
        SqlTypes.STRING,
        is(formatCode("String.valueOf(%s)"))
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCast() {
    assertCast(
        SqlTypes.DOUBLE,
        SqlTypes.decimal(2, 1),
        is(formatCode("(DecimalUtil.cast(%s, 2, 1))"))
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalCastNoOp() {
    assertCast(
        SqlTypes.decimal(2, 1),
        SqlTypes.decimal(2, 1),
        is(INNER_CODE)
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToIntCast() {
    assertCast(
        SqlTypes.decimal(2, 1),
        SqlTypes.INTEGER,
        is(formatCode("((%s).intValue())"))
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToLongCast() {
    assertCast(
        SqlTypes.decimal(2, 1),
        SqlTypes.BIGINT,
        is(formatCode("((%s).longValue())"))
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToDoubleCast() {
    assertCast(
        SqlTypes.decimal(2, 1),
        SqlTypes.DOUBLE,
        is(formatCode("((%s).doubleValue())"))
    );
  }

  @Test
  public void shouldGenerateCorrectCodeForDecimalToStringCast() {
    assertCast(
        SqlTypes.decimal(2, 1),
        SqlTypes.STRING,
        is(formatCode("%s.toPlainString()"))
    );
  }

  private void assertCast(
      final SqlType sourceType,
      final SqlType targetType,
      final Matcher<String> expectedCode
  ) {
    // When:
    final Pair<String, SqlType> result = CastEvaluator
        .eval(Pair.of(INNER_CODE, sourceType), targetType, ksqlConfig);

    // Then:
    assertThat(result.getLeft(), expectedCode);
    assertThat(result.getRight(), is(targetType));
  }

  private static String formatCode(final String code) {
    return String.format(code, INNER_CODE);
  }
}