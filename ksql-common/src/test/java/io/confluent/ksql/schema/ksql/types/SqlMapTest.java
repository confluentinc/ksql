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
import io.confluent.ksql.schema.ksql.SqlBaseType;
import org.junit.Test;

public class SqlMapTest {

  private static final SqlType SOME_TYPE = SqlPrimitiveType.of(SqlBaseType.DOUBLE);

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
}