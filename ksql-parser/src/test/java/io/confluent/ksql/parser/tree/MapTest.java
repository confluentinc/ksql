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

package io.confluent.ksql.parser.tree;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import io.confluent.ksql.parser.tree.Type.SqlType;
import org.junit.Test;

public class MapTest {

  private static final PrimitiveType SOME_TYPE = PrimitiveType.of(SqlType.DOUBLE);

  @Test
  public void shouldImplementHashCodeAndEqualsProperly() {
    new EqualsTester()
        .addEqualityGroup(Map.of(SOME_TYPE), Map.of(SOME_TYPE))
        .addEqualityGroup(Map.of(PrimitiveType.of(SqlType.BOOLEAN)))
        .addEqualityGroup(Array.of(PrimitiveType.of(SqlType.BOOLEAN)))
        .testEquals();
  }

  @Test
  public void shouldReturnSqlType() {
    assertThat(Map.of(SOME_TYPE).getSqlType(), is(SqlType.MAP));
  }

  @Test
  public void shouldReturnValueType() {
    assertThat(Map.of(SOME_TYPE).getValueType(), is(SOME_TYPE));
  }
}