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
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SqlMapTest {

  private static final SqlType SOME_TYPE = SqlPrimitiveType.of(SqlBaseType.DOUBLE);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(SqlMap.of(SOME_TYPE), SqlMap.of(SOME_TYPE))
        .addEqualityGroup(SqlMap.of(SqlPrimitiveType.of(SqlBaseType.BOOLEAN)))
        .addEqualityGroup(SqlPrimitiveType.of(SqlBaseType.BOOLEAN))
        .testEquals();
  }

  @Test
  public void shouldReturnBaseType() {
    assertThat(SqlMap.of(SOME_TYPE).baseType(), is(SqlBaseType.MAP));
  }

  @Test
  public void shouldReturnValueType() {
    assertThat(SqlMap.of(SOME_TYPE).getValueType(), is(SOME_TYPE));
  }

  @Test
  public void shouldImplementToString() {
    assertThat(SqlMap.of(SOME_TYPE).toString(), is(
        "MAP<STRING, "
            + SOME_TYPE
            + ">"
    ));
  }

  @Test
  public void shouldThrowIfValueNotMap() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.BIGINT);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("Expected MAP, got BIGINT");

    // When:
    schema.validateValue(10L);
  }

  @Test
  public void shouldThrowIfAnyKeyInValueNotKeyType() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.BIGINT);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("MAP key: Expected STRING, got INT");

    // When:
    schema.validateValue(ImmutableMap.of("first", 9L, 2, 9L));
  }

  @Test
  public void shouldThrowIfAnyValueInValueNotValueType() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.BIGINT);

    // Then:
    expectedException.expect(DataException.class);
    expectedException.expectMessage("MAP value for key '2': Expected BIGINT, got INT");

    // When:
    schema.validateValue(ImmutableMap.of("1", 11L, "2", 9));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullValue() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.BIGINT);

    // When:
    schema.validateValue(null);
  }

  @Test
  public void shouldValidateValue() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.BIGINT);
    final Map<Object, Object> mapWithNull = new HashMap<>();
    mapWithNull.put("valid", 44L);
    mapWithNull.put(null, 44L);
    mapWithNull.put("v", null);

    // When:
    schema.validateValue(mapWithNull);
  }
}