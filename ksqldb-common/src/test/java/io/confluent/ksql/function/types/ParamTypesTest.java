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

package io.confluent.ksql.function.types;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class ParamTypesTest {

  @Test
  public void shouldFailINonCompatibleSchemas() {
    assertThat(ParamTypes.areCompatible(SqlTypes.STRING, ParamTypes.INTEGER), is(false));

    assertThat(ParamTypes.areCompatible(SqlTypes.STRING, GenericType.of("T")), is(false));

    assertThat(
        ParamTypes.areCompatible(SqlTypes.array(SqlTypes.INTEGER), ArrayType.of(ParamTypes.STRING)),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlTypes.struct().field("a", SqlTypes.decimal(1, 1)).build(),
        StructType.builder().field("a", ParamTypes.DOUBLE).build()),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlTypes.map(SqlTypes.STRING, SqlTypes.decimal(1, 1)),
        MapType.of(ParamTypes.STRING, ParamTypes.INTEGER)),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlTypes.map(SqlTypes.decimal(1, 1), SqlTypes.INTEGER),
        MapType.of(ParamTypes.INTEGER, ParamTypes.INTEGER)),
        is(false));
  }

  @Test
  public void shouldPassCompatibleSchemas() {
    assertThat(ParamTypes.areCompatible(SqlTypes.STRING, ParamTypes.STRING),
        is(true));

    assertThat(
        ParamTypes.areCompatible(SqlTypes.array(SqlTypes.INTEGER), ArrayType.of(ParamTypes.INTEGER)),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlTypes.struct().field("a", SqlTypes.decimal(1, 1)).build(),
        StructType.builder().field("a", ParamTypes.DECIMAL).build()),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlTypes.map(SqlTypes.INTEGER, SqlTypes.decimal(1, 1)),
        MapType.of(ParamTypes.INTEGER, ParamTypes.DECIMAL)),
        is(true));
  }

  @Test
  public void shouldPassCompatibleSchemasWithImplicitCasting() {
    assertThat(ParamTypes.areCompatible(SqlTypes.INTEGER, ParamTypes.LONG, true), is(true));
    assertThat(ParamTypes.areCompatible(SqlTypes.INTEGER, ParamTypes.DOUBLE, true), is(true));
    assertThat(ParamTypes.areCompatible(SqlTypes.INTEGER, ParamTypes.DECIMAL, true), is(true));

    assertThat(ParamTypes.areCompatible(SqlTypes.BIGINT, ParamTypes.DOUBLE, true), is(true));
    assertThat(ParamTypes.areCompatible(SqlTypes.BIGINT, ParamTypes.DECIMAL, true), is(true));

    assertThat(ParamTypes.areCompatible(SqlTypes.decimal(2, 1), ParamTypes.DOUBLE, true), is(true));
  }

  @Test
  public void shouldNotPassInCompatibleSchemasWithImplicitCasting() {
    assertThat(ParamTypes.areCompatible(SqlTypes.BIGINT, ParamTypes.INTEGER, true), is(false));

    assertThat(ParamTypes.areCompatible(SqlTypes.DOUBLE, ParamTypes.LONG, true), is(false));

    assertThat(ParamTypes.areCompatible(SqlTypes.DOUBLE, ParamTypes.DECIMAL, true), is(false));
  }
}

