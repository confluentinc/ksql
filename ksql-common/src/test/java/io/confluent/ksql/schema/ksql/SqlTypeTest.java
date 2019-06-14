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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Test;

public class SqlTypeTest {

  private static final Set<SqlType> NUMBER_TYPES =
      ImmutableSet.of(SqlType.INTEGER, SqlType.BIGINT, SqlType.DOUBLE);

  @Test
  public void shouldNotBeNumber() {
    nonNumberTypes().forEach(sqlType ->
        assertThat(sqlType + " should not be number", sqlType.isNumber(), is(false)));
  }

  @Test
  public void shouldBeNumber() {
    numberTypes().forEach(sqlType ->
        assertThat(sqlType + " should be number", sqlType.isNumber(), is(true)));
  }

  @Test
  public void shouldNotUpCastIfNotNumber() {
    nonNumberTypes().forEach(sqlType ->
        assertThat(sqlType + " should not upcast", sqlType.canUpCast(sqlType), is(false)));
  }

  @Test
  public void shouldUpCastToSelfIfNumber() {
    numberTypes().forEach(sqlType ->
        assertThat(sqlType + " should upcast to self", sqlType.canUpCast(sqlType), is(true)));
  }

  @Test
  public void shouldUpCastInt() {
    assertThat(SqlType.INTEGER.canUpCast(SqlType.BIGINT), is(true));
    assertThat(SqlType.INTEGER.canUpCast(SqlType.DOUBLE), is(true));
  }

  @Test
  public void shouldUpCastBigInt() {
    assertThat(SqlType.BIGINT.canUpCast(SqlType.DOUBLE), is(true));
  }

  @Test
  public void shouldNotDownCastBigInt() {
    assertThat(SqlType.BIGINT.canUpCast(SqlType.INTEGER), is(false));
  }

  @Test
  public void shouldNotDownCastDouble() {
    assertThat(SqlType.DOUBLE.canUpCast(SqlType.INTEGER), is(false));
    assertThat(SqlType.DOUBLE.canUpCast(SqlType.BIGINT), is(false));
  }

  private static Stream<SqlType> numberTypes() {
    return NUMBER_TYPES.stream();
  }

  private static Stream<SqlType> nonNumberTypes() {
    return Arrays.stream(SqlType.values())
        .filter(sqlType -> !NUMBER_TYPES.contains(sqlType));
  }
}