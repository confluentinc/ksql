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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class DefaultSqlValueCoercerTest {

  private DefaultSqlValueCoercer coercer;

  @Before
  public void setUp() {
    coercer = new DefaultSqlValueCoercer();
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnArray() {
    coercer.coerce(ImmutableList.of(), SqlType.ARRAY);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnMap() {
    coercer.coerce(ImmutableMap.of(), SqlType.MAP);
  }

  @Test(expected = KsqlException.class)
  public void shouldThrowOnStruct() {
    coercer.coerce(new Struct(SchemaBuilder.struct()), SqlType.STRUCT);
  }

  @Test
  public void shouldOnlyCoerceNonNumberTypesToSelf() {
    ImmutableMap.of(
        SqlType.BOOLEAN, true,
        SqlType.STRING, "self"
    ).forEach((sqlType, value) -> {

      assertThat(coercer.coerce(value, sqlType), is(Optional.of(value)));
      assertThat(coercer.coerce(value, SqlType.INTEGER), is(Optional.empty()));
    });
  }

  @Test
  public void shouldUpCastInt() {
    // Given:
    final int val = 1;

    // Then:
    assertThat(coercer.coerce(val, SqlType.INTEGER), is(Optional.of(1)));
    assertThat(coercer.coerce(val, SqlType.BIGINT), is(Optional.of(1L)));
    assertThat(coercer.coerce(val, SqlType.DOUBLE), is(Optional.of(1D)));
  }

  @Test
  public void shouldUpCastBigInt() {
    // Given:
    final long val = 1L;

    // Then:
    assertThat(coercer.coerce(val, SqlType.BIGINT), is(Optional.of(1L)));
    assertThat(coercer.coerce(val, SqlType.DOUBLE), is(Optional.of(1D)));
  }

  @Test
  public void shouldUpCastDouble() {
    // Given:
    final double val = 1D;

    // Then:
    assertThat(coercer.coerce(val, SqlType.DOUBLE), is(Optional.of(1D)));
  }

  @Test
  public void shouldNotDownCastLong() {
    // Given:
    final long val = 1L;

    // Expect:
    assertThat(coercer.coerce(val, SqlType.INTEGER), is(Optional.empty()));
  }

  @Test
  public void shouldNotDownCastDouble() {
    // Given:
    final double val = 1d;

    // Expect:
    assertThat(coercer.coerce(val, SqlType.INTEGER), is(Optional.empty()));
    assertThat(coercer.coerce(val, SqlType.BIGINT), is(Optional.empty()));
  }
}