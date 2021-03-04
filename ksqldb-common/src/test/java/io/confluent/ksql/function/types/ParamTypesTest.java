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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlIntervalUnit;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class ParamTypesTest {

  @Test
  public void shouldFailINonCompatibleSchemas() {
    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.STRING),
        ParamTypes.INTEGER,
        false),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.STRING),
        GenericType.of("T"),
        false),
        is(false));

    assertThat(
        ParamTypes.areCompatible(
            SqlArgument.of(SqlTypes.array(SqlTypes.INTEGER)),
            ArrayType.of(ParamTypes.STRING),
            false),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.struct().field("a", SqlTypes.decimal(1, 1)).build()),
        StructType.builder().field("a", ParamTypes.DOUBLE).build(),
        false),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.map(SqlTypes.STRING, SqlTypes.decimal(1, 1))),
        MapType.of(ParamTypes.STRING, ParamTypes.INTEGER),
        false),
        is(false));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.map(SqlTypes.decimal(1, 1), SqlTypes.INTEGER)),
        MapType.of(ParamTypes.INTEGER, ParamTypes.INTEGER),
        false),
        is(false));


    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.INTEGER), SqlTypes.INTEGER)),
        LambdaType.of(ImmutableList.of(ParamTypes.STRING), ParamTypes.STRING),
        false),
        is(false));
  }

  @Test
  public void shouldPassCompatibleSchemas() {
    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.STRING),
        ParamTypes.STRING,
        false),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlIntervalUnit.INSTANCE),
        ParamTypes.INTERVALUNIT,
        true),
        is(true));

    assertThat(
        ParamTypes.areCompatible(
            SqlArgument.of(SqlTypes.array(SqlTypes.INTEGER)),
            ArrayType.of(ParamTypes.INTEGER),
            false),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.struct().field("a", SqlTypes.decimal(1, 1)).build()),
        StructType.builder().field("a", ParamTypes.DECIMAL).build(),
        false),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlTypes.map(SqlTypes.INTEGER, SqlTypes.decimal(1, 1))),
        MapType.of(ParamTypes.INTEGER, ParamTypes.DECIMAL),
        false),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlLambdaResolved.of(ImmutableList.of(SqlTypes.STRING), SqlTypes.STRING)),
        LambdaType.of(ImmutableList.of(ParamTypes.STRING), ParamTypes.STRING),
        false),
        is(true));
  }

  @Test
  public void shouldOnlyLookAtInputTypeSizeForCompatibleLambdaTypeAndSqlLambda() {
    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlLambda.of(1)),
        LambdaType.of(ImmutableList.of(ParamTypes.STRING), ParamTypes.STRING),
        false),
        is(true));

    assertThat(ParamTypes.areCompatible(
        SqlArgument.of(SqlLambda.of(2)),
        LambdaType.of(ImmutableList.of(ParamTypes.DOUBLE), ParamTypes.STRING),
        false),
        is(false));
  }

  @Test
  public void shouldPassCompatibleSchemasWithImplicitCasting() {
    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.INTEGER), ParamTypes.LONG, true), is(true));
    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.INTEGER), ParamTypes.DOUBLE, true), is(true));
    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.INTEGER), ParamTypes.DECIMAL, true), is(true));

    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.BIGINT), ParamTypes.DOUBLE, true), is(true));
    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.BIGINT), ParamTypes.DECIMAL, true), is(true));

    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.decimal(2, 1)), ParamTypes.DOUBLE, true), is(true));
  }

  @Test
  public void shouldNotPassInCompatibleSchemasWithImplicitCasting() {
    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.BIGINT), ParamTypes.INTEGER, true), is(false));

    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.DOUBLE), ParamTypes.LONG, true), is(false));

    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.DOUBLE), ParamTypes.DECIMAL, true), is(false));
    assertThat(ParamTypes.areCompatible(SqlArgument.of(SqlTypes.INTEGER), ParamTypes.INTERVALUNIT, true), is(false));
  }
}

