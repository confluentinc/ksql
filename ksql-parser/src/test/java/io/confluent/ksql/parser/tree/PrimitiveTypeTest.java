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
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.util.KsqlException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PrimitiveTypeTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Unknown primitive type: WHAT_IS_THIS?");

    // When:
    PrimitiveType.of("WHAT_IS_THIS?");
  }

  @Test
  public void shouldThrowOnArrayType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid primitive type: ARRAY");

    // When:
    PrimitiveType.of(SqlType.ARRAY);
  }

  @Test
  public void shouldThrowOnMapType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid primitive type: MAP");

    // When:
    PrimitiveType.of(SqlType.MAP);
  }

  @Test
  public void shouldThrowOnStructType() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Invalid primitive type: STRUCT");

    // When:
    PrimitiveType.of(SqlType.STRUCT);
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