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
        .addEqualityGroup(SqlMap.of(SOME_TYPE.required(), SOME_TYPE.required()).required(),
                SqlMap.of(SOME_TYPE.required(), SOME_TYPE.required()).required())
        .addEqualityGroup(SqlMap.of(OTHER_TYPE.required(), SOME_TYPE.required()).required())
        .addEqualityGroup(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE.required()).required())
        .addEqualityGroup(SOME_TYPE.required())
        .testEquals();
  }

  @Test
  public void shouldReturnBaseType() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).baseType(), is(SqlBaseType.MAP));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).isOptional(), is(true));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).required().baseType(), is(SqlBaseType.MAP));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).required().isOptional(), is(false));
  }

  @Test
  public void shouldReturnKeyType() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).getKeyType(), is(SOME_TYPE));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).getKeyType().isOptional(), is(true));
    assertThat(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE).getKeyType(), is(SOME_TYPE.required()));
    assertThat(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE).getKeyType().isOptional(), is(false));
  }

  @Test
  public void shouldReturnValueType() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).getValueType(), is(OTHER_TYPE));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE.required()).getValueType(), is(OTHER_TYPE.required()));
  }

  @Test
  public void shouldImplementToString() {
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).toString(), is("MAP<DOUBLE, INTEGER>"));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE.required()).toString(), is("MAP<DOUBLE, INTEGER NOT NULL>"));
    assertThat(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE).toString(), is("MAP<DOUBLE NOT NULL, INTEGER>"));
    assertThat(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE.required()).toString(), is("MAP<DOUBLE NOT NULL, INTEGER NOT NULL>"));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE).required().toString(), is("MAP<DOUBLE, INTEGER> NOT NULL"));
    assertThat(SqlMap.of(SOME_TYPE, OTHER_TYPE.required()).required().toString(), is("MAP<DOUBLE, INTEGER NOT NULL> NOT NULL"));
    assertThat(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE).required().toString(), is("MAP<DOUBLE NOT NULL, INTEGER> NOT NULL"));
    assertThat(SqlMap.of(SOME_TYPE.required(), OTHER_TYPE.required()).required().toString(), is("MAP<DOUBLE NOT NULL, INTEGER NOT NULL> NOT NULL"));

  }
}