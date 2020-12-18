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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.schema.utils.SchemaException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class SqlPrimitiveTypeTest {

  @SuppressWarnings("UnstableApiUsage")
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
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.TIMESTAMP),
            SqlPrimitiveType.of(SqlBaseType.TIMESTAMP))
        .addEqualityGroup(SqlArray.of(SqlPrimitiveType.of(SqlBaseType.STRING)))
        .testEquals();
  }

  @Test
  public void shouldReturnSqlType() {
    assertThat(SqlPrimitiveType.of(SqlBaseType.INTEGER).baseType(), is(SqlBaseType.INTEGER));
  }

  @Test
  public void shouldThrowOnUnknownTypeString() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> SqlPrimitiveType.of("WHAT_IS_THIS?")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown primitive type: WHAT_IS_THIS?"));
  }

  @Test
  public void shouldThrowOnArrayType() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> SqlPrimitiveType.of(SqlBaseType.ARRAY)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid primitive type: ARRAY"));
  }

  @Test
  public void shouldThrowOnMapType() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> SqlPrimitiveType.of(SqlBaseType.MAP)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid primitive type: MAP"));
  }

  @Test
  public void shouldThrowOnStructType() {
    // When:
    final Exception e = assertThrows(
        SchemaException.class,
        () -> SqlPrimitiveType.of(SqlBaseType.STRUCT)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid primitive type: STRUCT"));
  }

  @Test
  public void shouldSupportSqlPrimitiveTypes() {
    // Given:
    final java.util.Map<String, SqlBaseType> primitives = new ImmutableMap.Builder<String, SqlBaseType>()
        .put("BooleaN", SqlBaseType.BOOLEAN)
        .put("IntegeR", SqlBaseType.INTEGER)
        .put("BigInT", SqlBaseType.BIGINT)
        .put("DoublE", SqlBaseType.DOUBLE)
        .put("StrinG", SqlBaseType.STRING)
        .put("tImeStamP", SqlBaseType.TIMESTAMP)
        .build();

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
        "STRING",
        "TIMESTAMP"
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
        SqlBaseType.STRING,
        SqlBaseType.TIMESTAMP
    ).forEach(type -> {
      // Then:
      assertThat(SqlPrimitiveType.of(type).toString(), is(type.toString()));
    });
  }
}