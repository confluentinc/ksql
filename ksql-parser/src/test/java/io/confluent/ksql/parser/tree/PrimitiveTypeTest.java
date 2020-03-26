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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class PrimitiveTypeTest {

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(PrimitiveType.of(SqlType.BOOLEAN), PrimitiveType.of(SqlType.BOOLEAN))
        .addEqualityGroup(PrimitiveType.of(SqlType.INTEGER), PrimitiveType.of(SqlType.INTEGER))
        .addEqualityGroup(PrimitiveType.of(SqlType.BIGINT), PrimitiveType.of(SqlType.BIGINT))
        .addEqualityGroup(PrimitiveType.of(SqlType.DOUBLE), PrimitiveType.of(SqlType.DOUBLE))
        .addEqualityGroup(PrimitiveType.of(SqlType.STRING), PrimitiveType.of(SqlType.STRING))
        .addEqualityGroup(Array.of(PrimitiveType.of(SqlType.STRING)))
        .testEquals();
  }

  @Test
  public void shouldReturnSqlType() {
    assertThat(PrimitiveType.of(SqlType.INTEGER).getSqlType(), is(SqlType.INTEGER));
  }

  @Test
  public void shouldThrowOnUnknownTypeString() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> PrimitiveType.of("WHAT_IS_THIS?")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown primitive type: WHAT_IS_THIS?"));
  }

  @Test
  public void shouldThrowOnArrayType() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> PrimitiveType.of(SqlType.ARRAY)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid primitive type: ARRAY"));
  }

  @Test
  public void shouldThrowOnMapType() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> PrimitiveType.of(SqlType.MAP)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid primitive type: MAP"));
  }

  @Test
  public void shouldThrowOnStructType() {
    // When:
    final KsqlException e = assertThrows(
        (KsqlException.class),
        () -> PrimitiveType.of(SqlType.STRUCT)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid primitive type: STRUCT"));
  }

  @Test
  public void shouldSupportPrimitiveTypes() {
    // Given:
    final java.util.Map<String, SqlType> primitives = ImmutableMap.of(
        "BooleaN", SqlType.BOOLEAN,
        "IntegeR", SqlType.INTEGER,
        "BigInT", SqlType.BIGINT,
        "DoublE", SqlType.DOUBLE,
        "StrinG", SqlType.STRING
    );

    primitives.forEach((string, expected) ->
        // Then:
        assertThat(PrimitiveType.of(string).getSqlType(), is(expected))
    );
  }

  @Test
  public void shouldSupportAlternativePrimitiveTypeNames() {
    // Given:
    final java.util.Map<String, SqlType> primitives = ImmutableMap.of(
        "InT", SqlType.INTEGER,
        "VarchaR", SqlType.STRING
    );

    primitives.forEach((string, expected) ->
        // Then:
        assertThat(PrimitiveType.of(string).getSqlType(), is(expected))
    );
  }
}