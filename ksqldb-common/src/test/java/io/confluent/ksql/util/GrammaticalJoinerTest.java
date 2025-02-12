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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.junit.Test;


public class GrammaticalJoinerTest {

  private final GrammaticalJoiner joiner = GrammaticalJoiner.or();

  @Test
  public void shouldHandleEmptyList() {
    assertThat(joiner.join(ImmutableList.of()), is(""));
  }

  @Test
  public void shouldHandleSingleItem() {
    assertThat(joiner.join(ImmutableList.of(1)), is("1"));
  }

  @Test
  public void shouldHandleTwoItems() {
    assertThat(joiner.join(ImmutableList.of(1, 2)), is("1 or 2"));
  }

  @Test
  public void shouldHandleThreeItems() {
    assertThat(joiner.join(ImmutableList.of(1, 2, 3)), is("1, 2 or 3"));
  }

  @Test
  public void shouldHandleFourItems() {
    assertThat(joiner.join(ImmutableList.of(1, 2, 3, 4)), is("1, 2, 3 or 4"));
  }

  @Test
  public void shouldHandleNulls() {
    assertThat(joiner.join(Arrays.asList("a", null, "c")), is("a, null or c"));
  }

  @Test
  public void shouldBuildWithAnd() {
    assertThat(GrammaticalJoiner.and().join(Arrays.asList(1, 2, 3)), is("1, 2 and 3"));
  }

  @Test
  public void shouldBuildWithComma() {
    assertThat(GrammaticalJoiner.comma().join(Arrays.asList(1, 2, 3)), is("1, 2, 3"));
  }
}