/*
 * Copyright 2021 Confluent Inc.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Test;

public class SqlBaseTypeTest {

  private static final Set<SqlBaseType> NUMBER_TYPES = ImmutableSet.of(
      SqlBaseType.INTEGER,
      SqlBaseType.BIGINT,
      SqlBaseType.DECIMAL,
      SqlBaseType.DOUBLE
  );

  @Test
  public void shouldNotBeNumber() {
    nonNumberTypes().forEach(sqlType -> assertThat(
        sqlType + " should not be number",
        sqlType.isNumber(),
        is(false)
    ));
  }

  @Test
  public void shouldBeNumber() {
    numberTypes().forEach(sqlType -> assertThat(
        sqlType + " should be number",
        sqlType.isNumber(),
        is(true)
    ));
  }

  @Test
  public void shouldNotUpCastIfNotNumber() {
    nonNumberTypes().forEach(sqlType -> assertThat(
        sqlType + " should not upcast",
        sqlType.canImplicitlyCast(SqlBaseType.DOUBLE),
        is(false))
    );
  }

  @Test
  public void shouldUpCastIfNumber() {
    numberTypes().forEach(sqlType -> assertThat(
        sqlType + " should upcast",
        sqlType.canImplicitlyCast(SqlBaseType.DOUBLE),
        is(true))
    );
  }

  @Test
  public void shouldUpCastToSelf() {
    allTypes().forEach(sqlType ->
        assertThat(sqlType + " should upcast to self", sqlType.canImplicitlyCast(sqlType), is(true)));
  }

  @Test
  public void shouldUpCastInt() {
    assertThat(SqlBaseType.INTEGER.canImplicitlyCast(SqlBaseType.BIGINT), is(true));
    assertThat(SqlBaseType.INTEGER.canImplicitlyCast(SqlBaseType.DECIMAL), is(true));
    assertThat(SqlBaseType.INTEGER.canImplicitlyCast(SqlBaseType.DOUBLE), is(true));
  }

  @Test
  public void shouldUpCastBigInt() {
    assertThat(SqlBaseType.BIGINT.canImplicitlyCast(SqlBaseType.DECIMAL), is(true));
    assertThat(SqlBaseType.BIGINT.canImplicitlyCast(SqlBaseType.DOUBLE), is(true));
  }

  @Test
  public void shouldUpCastDecimal() {
    assertThat(SqlBaseType.DECIMAL.canImplicitlyCast(SqlBaseType.DOUBLE), is(true));
  }

  @Test
  public void shouldNotDownCastBigInt() {
    assertThat(SqlBaseType.BIGINT.canImplicitlyCast(SqlBaseType.INTEGER), is(false));
  }

  @Test
  public void shouldNotDownCastDecimal() {
    assertThat(SqlBaseType.DECIMAL.canImplicitlyCast(SqlBaseType.INTEGER), is(false));
    assertThat(SqlBaseType.DECIMAL.canImplicitlyCast(SqlBaseType.BIGINT), is(false));
  }

  @Test
  public void shouldNotDownCastDouble() {
    assertThat(SqlBaseType.DOUBLE.canImplicitlyCast(SqlBaseType.INTEGER), is(false));
    assertThat(SqlBaseType.DOUBLE.canImplicitlyCast(SqlBaseType.BIGINT), is(false));
    assertThat(SqlBaseType.DOUBLE.canImplicitlyCast(SqlBaseType.DECIMAL), is(false));
  }

  private static Stream<SqlBaseType> numberTypes() {
    return NUMBER_TYPES.stream();
  }

  private static Stream<SqlBaseType> nonNumberTypes() {
    return Arrays.stream(SqlBaseType.values())
        .filter(sqlType -> !NUMBER_TYPES.contains(sqlType));
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Stream<SqlBaseType> allTypes() {
    return Streams.concat(numberTypes(), nonNumberTypes());
  }
}