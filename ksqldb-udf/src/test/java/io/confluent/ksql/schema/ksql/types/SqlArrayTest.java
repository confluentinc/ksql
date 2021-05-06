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

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class SqlArrayTest {

  private static final SqlType SOME_TYPE = SqlPrimitiveType.of(SqlBaseType.STRING);

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(SqlArray.of(SOME_TYPE), SqlArray.of(SOME_TYPE))
        .addEqualityGroup(SqlArray.of(SqlPrimitiveType.of(SqlBaseType.BOOLEAN)))
        .addEqualityGroup(SqlMap.of(SqlTypes.STRING, SqlTypes.BOOLEAN))
        .addEqualityGroup(SqlArray.of(SOME_TYPE.required()).required(), SqlArray.of(SOME_TYPE.required()).required())
        .addEqualityGroup(SqlArray.of(SqlPrimitiveType.of(SqlBaseType.BOOLEAN).required()).required())
        .addEqualityGroup(SqlMap.of(SqlTypes.STRING.required(), SqlTypes.BOOLEAN.required()).required())
        .testEquals();
  }

  @Test
  public void shouldReturnSqlType() {
    assertThat(SqlArray.of(SOME_TYPE).baseType(), is(SqlBaseType.ARRAY));
    assertThat(SqlArray.of(SOME_TYPE).isOptional(), is(true));
    assertThat(SqlArray.of(SOME_TYPE).required().baseType(), is(SqlBaseType.ARRAY));
    assertThat(SqlArray.of(SOME_TYPE).required().isOptional(), is(false));
  }

  @Test
  public void shouldReturnValueType() {
    assertThat(SqlArray.of(SOME_TYPE).getItemType(), is(SOME_TYPE));
    assertThat(SqlArray.of(SOME_TYPE).getItemType().isOptional(), is(true));
    assertThat(SqlArray.of(SOME_TYPE.required()).getItemType(), is(SOME_TYPE.required()));
    assertThat(SqlArray.of(SOME_TYPE.required()).getItemType().isOptional(), is(false));
  }

  @Test
  public void shouldImplementToString() {
    assertThat(SqlArray.of(SOME_TYPE).toString(), is(
        "ARRAY<"
            + SOME_TYPE.toString()
            + ">"
    ));
    assertThat(SqlArray.of(SOME_TYPE.required()).toString(), is(
            "ARRAY<"
                    + SOME_TYPE.toString() + " NOT NULL"
                    + ">"
    ));
    assertThat(SqlArray.of(SOME_TYPE).required().toString(), is(
            "ARRAY<"
                    + SOME_TYPE.toString()
                    + "> NOT NULL"
    ));
  }
}