/*
 * Copyright 2018 Confluent Inc.
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

public class SplitTest {
  private final static Split splitUdf = new Split();

  @Test
  public void shouldReturnNullOnAnyNullParameters() {
    assertThat(splitUdf.split(null, ""), is(nullValue()));
    assertThat(splitUdf.split("", null), is(nullValue()));
    assertThat(splitUdf.split(null, null), is(nullValue()));
  }

  @Test
  public void shouldReturnOriginalStringOnNotFoundDelimiter() {
    assertThat(splitUdf.split("", "."), contains(""));
    assertThat(splitUdf.split("x-y", "."), contains("x-y"));
  }

  @Test
  public void shouldSplitAllCharactersByGivenAnEmptyDelimiter() {
    assertThat(splitUdf.split("", ""), contains(""));
    assertThat(splitUdf.split("x-y", ""), contains("x", "-", "y"));
  }

  @Test
  public void shouldSplitStringByGivenDelimiter() {
    assertThat(splitUdf.split("x-y", "-"), contains("x", "y"));
    assertThat(splitUdf.split("x-y", "x"), contains("", "-y"));
    assertThat(splitUdf.split("x-y", "y"), contains("x-", ""));
    assertThat(splitUdf.split("a.b.c.d", "."), contains("a", "b", "c", "d"));

  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterIsFoundAtTheBeginningOrEnd() {
    assertThat(splitUdf.split("$A", "$"), contains("", "A"));
    assertThat(splitUdf.split("$A$B", "$"), contains("", "A", "B"));
    assertThat(splitUdf.split("A$", "$"), contains("A", ""));
    assertThat(splitUdf.split("A$B$", "$"), contains("A", "B", ""));
    assertThat(splitUdf.split("$A$B$", "$"), contains("", "A", "B", ""));
  }

  @Test
  public void shouldSplitAndAddEmptySpacesIfDelimiterIsFoundInContiguousPositions() {
    assertThat(splitUdf.split("A||A", "|"), contains("A", "", "A"));
    assertThat(splitUdf.split("z||A||z", "|"), contains("z", "", "A", "", "z"));
    assertThat(splitUdf.split("||A||A", "|"), contains("", "", "A", "", "A"));
    assertThat(splitUdf.split("A||A||", "|"), contains("A", "", "A", "", ""));
  }
}
