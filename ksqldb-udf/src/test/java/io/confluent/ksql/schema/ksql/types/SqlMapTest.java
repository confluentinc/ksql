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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.schema.utils.DataException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SqlMapTest {

  private static final SqlType SOME_TYPE = SqlPrimitiveType.of(SqlBaseType.DOUBLE);
  private static final SqlType OTHER_TYPE = SqlPrimitiveType.of(SqlBaseType.INTEGER);

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(SqlMap.of(SOME_TYPE, SOME_TYPE), SqlMap.of(SOME_TYPE, SOME_TYPE))
        .addEqualityGroup(SqlMap.of(OTHER_TYPE, SOME_TYPE))
        .addEqualityGroup(SqlMap.of(SOME_TYPE, OTHER_TYPE))
        .addEqualityGroup(SOME_TYPE)
        .testEquals();
  }

  @Test
  public void shouldReturnBaseType() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).baseType(), is(SqlBaseType.MAP));
  }

  @Test
  public void shouldReturnKeyType() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).getKeyType(), is(SOME_TYPE));
  }

  @Test
  public void shouldReturnValueType() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).getValueType(), is(OTHER_TYPE));
  }

  @Test
  public void shouldImplementToString() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).toString(), is("MAP<DOUBLE, INTEGER>"));
  }

  @Test
  public void shouldThrowIfValueNotMap() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT);

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> schema.validateValue(10L)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Expected MAP, got BIGINT"));
  }

  @Test
  public void shouldThrowIfAnyKeyInValueNotKeyType() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT);

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> schema.validateValue(ImmutableMap.of("first", 9L, 2, 9L))
    );

    // Then:
    assertThat(e.getMessage(), containsString("MAP key: Expected STRING, got INT"));
  }

  @Test
  public void shouldThrowIfAnyValueInValueNotValueType() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT);

    // When:
    final DataException e = assertThrows(
        DataException.class,
        () -> schema.validateValue(ImmutableMap.of("1", 11L, "2", 9))
    );

    // Then:
    assertThat(e.getMessage(), containsString("MAP value for key '2': Expected BIGINT, got INT"));
  }

  @Test
  public void shouldNotThrowWhenValidatingNullValue() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT);

    // When:
    schema.validateValue(null);
  }

  @Test
  public void shouldValidateValue() {
    // Given:
    final SqlMap schema = SqlTypes.map(SqlTypes.STRING, SqlTypes.BIGINT);
    final Map<Object, Object> mapWithNull = new HashMap<>();
    mapWithNull.put("valid", 44L);
    mapWithNull.put(null, 44L);
    mapWithNull.put("v", null);

    // When:
    schema.validateValue(mapWithNull);
  }
}