/*
 * Copyright 2020 Confluent Inc.
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
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class SqlDoublesTest {

  @Test
  public void shouldParseDouble() {
    assertThat(SqlDoubles.parseDouble("1.3"), is(1.3D));
  }

  @Test
  public void shouldThrowIfNotNumber() {
    assertThrows(NumberFormatException.class, () -> SqlDoubles.parseDouble("What no number?"));
  }

  @Test
  public void shouldThrowOnInfinity() {
    assertThrows(NumberFormatException.class, () -> SqlDoubles.parseDouble("Infinity"));
    assertThrows(NumberFormatException.class, () -> SqlDoubles.parseDouble("-Infinity"));
  }

  @Test
  public void shouldThrowOnNaN() {
    assertThrows(NumberFormatException.class, () -> SqlDoubles.parseDouble("NaN"));
  }
}