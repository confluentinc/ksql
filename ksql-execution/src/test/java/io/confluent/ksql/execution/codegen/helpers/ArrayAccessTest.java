/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.codegen.helpers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.Test;

public class ArrayAccessTest {

  @Test
  public void shouldBeOneIndexed() {
    // Given:
    List<Integer> list = ImmutableList.of(1, 2);

    // When:
    Integer access = ArrayAccess.arrayAccess(list, 1);

    // Then:
    assertThat(access, is(1));
  }

  @Test
  public void shouldSupportNegativeIndex() {
    // Given:
    List<Integer> list = ImmutableList.of(1, 2);

    // When:
    Integer access = ArrayAccess.arrayAccess(list, -1);

    // Then:
    assertThat(access, is(2));
  }

  @Test
  public void shouldReturnNullOnOutOfBoundsIndex() {
    // Given:
    List<Integer> list = ImmutableList.of(1, 2);

    // When:
    Integer access = ArrayAccess.arrayAccess(list, 3);

    // Then:
    assertThat(access, nullValue());
  }

  @Test
  public void shouldReturnNullOnNegativeOutOfBoundsIndex() {
    // Given:
    List<Integer> list = ImmutableList.of(1, 2);

    // When:
    Integer access = ArrayAccess.arrayAccess(list, -3);

    // Then:
    assertThat(access, nullValue());
  }

}