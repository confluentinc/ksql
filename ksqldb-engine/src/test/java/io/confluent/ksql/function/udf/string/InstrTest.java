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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

public class InstrTest {

  private Instr udf;

  @Before
  public void setUp() {
    udf = new Instr();
  }

  @Test
  public void shouldReturnZeroOnNullValue() {
    assertThat(udf.instr(null, "OR"), is(0));
    assertThat(udf.instr(null, "OR", 1), is(0));
    assertThat(udf.instr(null, "OR", 1, 1), is(0));
    assertThat(udf.instr("CORPORATE FLOOR", null, 1), is(0));
    assertThat(udf.instr("CORPORATE FLOOR", null, 1, 1), is(0));
  }

  @Test
  public void shouldExtractFromStartForPositivePositions() {
    assertThat(udf.instr("CORPORATE FLOOR", "OR"), is(2));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", 3), is(5));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", 3, 2), is(14));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", 3, 5), is(0));

    assertThat(udf.instr("CORPORATE FLOOR", "ATE"), is(7));
    assertThat(udf.instr("CORPORATE FLOOR", "ATE", 2), is(7));
    assertThat(udf.instr("CORPORATE FLOOR", "ATE", 3, 2), is(0));
  }

  @Test
  public void shouldExtractFromEndForNegativePositions() {
    assertThat(udf.instr("CORPORATE FLOOR", "OR", -1), is(14));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", -3), is(5));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", -3, 2), is(2));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", -3, 5), is(0));

    assertThat(udf.instr("CORPORATE FLOOR", "ATE", -1), is(7));
    assertThat(udf.instr("CORPORATE FLOOR", "ATE", -3), is(7));
    assertThat(udf.instr("CORPORATE FLOOR", "ATE", -3, 2), is(0));
  }

  @Test
  public void shouldReturnZeroWhenSubstringNotFound() {
    assertThat(udf.instr("CORPORATE FLOOR", "ABC"), is(0));
  }

  @Test
  public void shouldTruncateOutOfBoundIndexes() {
    assertThat(udf.instr("CORPORATE FLOOR", "OR", 100), is(0));
    assertThat(udf.instr("CORPORATE FLOOR", "OR", -100), is(0));
  }
}
