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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.schema.ksql.DataException;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SqlPrimitiveTypeTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.BOOLEAN),
            SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.INTEGER),
            SqlPrimitiveType.of(SqlBaseType.INTEGER))
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.BIGINT),
            SqlPrimitiveType.of(SqlBaseType.BIGINT))
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.DOUBLE),
            SqlPrimitiveType.of(SqlBaseType.DOUBLE))
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.STRING),
            SqlPrimitiveType.of(SqlBaseType.STRING))
        .addEqualityGroup(SqlArray.of(SqlPrimitiveType.of(SqlBaseType.STRING)))
        .testEquals();
  }

  @Test
  public void shouldReturnSqlType() {
    assertThat(SqlPrimitiveType.of(SqlBaseType.INTEGER).baseType(), is(SqlBaseType.INTEGER));
  }

  @Test
  public void shouldThrowOnUnknownTypeString() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unknown primitive type: WHAT_IS_THIS?");

    // When:
    SqlPrimitiveType.of("WHAT_IS_THIS?");
  }

  @Test
  public void shouldThrowOnArrayType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid primitive type: ARRAY");

    // When:
    SqlPrimitiveType.of(SqlBaseType.ARRAY);
  }

  @Test
  public void shouldThrowOnMapType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid primitive type: MAP");

    // When:
    SqlPrimitiveType.of(SqlBaseType.MAP);
  }

  @Test
  public void shouldThrowOnStructType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid primitive type: STRUCT");

    // When:
    SqlPrimitiveType.of(SqlBaseType.STRUCT);
  }

  @Test
  public void shouldSupportSqlPrimitiveTypes() {
    // Given:
    final java.util.Map<String, SqlBaseType> primitives = ImmutableMap.of(
        "BooleaN", SqlBaseType.BOOLEAN,
        "IntegeR", SqlBaseType.INTEGER,
        "BigInT", SqlBaseType.BIGINT,
        "DoublE", SqlBaseType.DOUBLE,
        "StrinG", SqlBaseType.STRING
    );

    primitives.forEach((string, expected) ->
        // Then:
        assertThat(SqlPrimitiveType.of(string).baseType(), is(expected))
    );
  }

  @Test
  public void shouldSupportAlternativeSqlPrimitiveTypeNames() {
    // Given:
    final java.util.Map<String, SqlBaseType> primitives = ImmutableMap.of(
        "InT", SqlBaseType.INTEGER,
        "VarchaR", SqlBaseType.STRING
    );

    primitives.forEach((string, expected) ->
        // Then:
        assertThat(SqlPrimitiveType.of(string).baseType(), is(expected))
    );
  }

  @Test
  public void shouldSupportAllPrimitiveTypeNames() {
    // Given:
    final Set<String> typeNames = ImmutableSet.of(
        "INT",
        "VARCHAR",
        "BOOLEAN",
        "BIGINT",
        "DOUBLE",
        "STRING"
    );

    // When:
    final List<Boolean> missing = typeNames.stream()
        .map(SqlPrimitiveType::isPrimitiveTypeName)
        .filter(x -> !x)
        .collect(Collectors.toList());

    // Then:
    assertThat(missing, is(empty()));
  }

  @Test
  public void shouldNotSupportRandomTypeName() {
    // When:
    final boolean isPrimitive = SqlPrimitiveType.isPrimitiveTypeName("WILL ROBINSON");

    // Then:
    assertThat("expected not primitive!", !isPrimitive);
  }

  @Test
  public void shouldImplementToString() {
    ImmutableList.of(
        SqlBaseType.BOOLEAN,
        SqlBaseType.INTEGER,
        SqlBaseType.BIGINT,
        SqlBaseType.DOUBLE,
        SqlBaseType.STRING
    ).forEach(type -> {
      // Then:
      assertThat(SqlPrimitiveType.of(type).toString(), is(type.toString()));
    });
  }

  @Test
  public void shoudlValidatePrimitiveTypes() {
    SqlPrimitiveType.of(SqlBaseType.BOOLEAN).validateValue(true);
    SqlPrimitiveType.of(SqlBaseType.INTEGER).validateValue(19);
    SqlPrimitiveType.of(SqlBaseType.BIGINT).validateValue(33L);
    SqlPrimitiveType.of(SqlBaseType.DOUBLE).validateValue(45.0D);
    SqlPrimitiveType.of(SqlBaseType.STRING).validateValue("");
  }

  @SuppressWarnings("UnnecessaryBoxing")
  @Test
  public void shouldValidateBoxedTypes() {
    SqlPrimitiveType.of(SqlBaseType.BOOLEAN).validateValue(Boolean.FALSE);
    SqlPrimitiveType.of(SqlBaseType.INTEGER).validateValue(Integer.valueOf(19));
    SqlPrimitiveType.of(SqlBaseType.BIGINT).validateValue(Long.valueOf(33L));
    SqlPrimitiveType.of(SqlBaseType.DOUBLE).validateValue(Double.valueOf(45.0D));
  }

  @Test
  public void shouldValidateNullValue() {
    SqlPrimitiveType.of(SqlBaseType.BOOLEAN).validateValue(null);
  }

  @Test
  public void shouldFailValidationForWrongType() {
    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected BOOLEAN, got INT");

    // When:
    SqlPrimitiveType.of(SqlBaseType.BOOLEAN).validateValue(10);
  }
}