/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

@SuppressFBWarnings(
        value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
        justification = "get() throws exception"
)
public class VariadicArgsTest {

  @Test
  public void shouldThrowWhenIndexTooLarge() {
    final VariadicArgs<Integer> varArgs = new VariadicArgs<>(ImmutableList.of(1, 2, 3));

    final Exception e = assertThrows(
            IndexOutOfBoundsException.class,
            () -> varArgs.get(3)
    );

    assertThat(e.getMessage(), is("Attempted to access variadic argument at index 3 when only 3 "
            + "arguments are available"));
  }

  @Test
  public void shouldThrowWhenIndexNegative() {
    final VariadicArgs<Integer> varArgs = new VariadicArgs<>(ImmutableList.of(1, 2, 3));

    final Exception e = assertThrows(
            IndexOutOfBoundsException.class,
            () -> varArgs.get(-1)
    );

    assertThat(e.getMessage(), is("Attempted to access variadic argument at index -1 when only 3 "
            + "arguments are available"));
  }

  @Test
  public void shouldGetAllArgsByIndex() {
    final VariadicArgs<Integer> varArgs = new VariadicArgs<>(ImmutableList.of(1, 2, 3));

    final List<Integer> foundArgs = new ArrayList<>();
    for (int index = 0; index < varArgs.size(); index++) {
      foundArgs.add(varArgs.get(index));
    }

    assertEquals(Arrays.asList(1, 2, 3), foundArgs);
  }

  @Test
  public void shouldGetAllArgsByIteration() {
    final VariadicArgs<Integer> varArgs = new VariadicArgs<>(ImmutableList.of(1, 2, 3));

    final List<Integer> foundArgs = new ArrayList<>();
    for (int arg : varArgs) {
      foundArgs.add(arg);
    }

    assertEquals(Arrays.asList(1, 2, 3), foundArgs);
  }

  @Test
  public void shouldGetAllArgsByStream() {
    final VariadicArgs<Integer> varArgs = new VariadicArgs<>(ImmutableList.of(1, 2, 3));
    final List<Integer> foundArgs = varArgs.stream().collect(Collectors.toList());
    assertEquals(Arrays.asList(1, 2, 3), foundArgs);
  }

  @Test
  public void shouldCreateEmptyVarArgs() {
    final VariadicArgs<Integer> varArgs = new VariadicArgs<>(ImmutableList.of());
    assertEquals(0, varArgs.size());
  }

}