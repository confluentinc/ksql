/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

public class CompatibleSetTest {

  private static class InCompatibleInteger implements InCompatible<InCompatibleInteger> {

    final private Set<InCompatibleInteger> inCompatibleIntegers;
    final private int value;

    public InCompatibleInteger(final int value) {
      this.value = value;
      inCompatibleIntegers = ImmutableSet.of();
    }

    public InCompatibleInteger(final int value, final Integer... inCompatibleIntegers) {
      this.inCompatibleIntegers = Arrays.stream(inCompatibleIntegers)
          .map(i -> new InCompatibleInteger(i))
          .collect(ImmutableSet.toImmutableSet());
      this.value = value;
    }

    public Set<InCompatibleInteger> getIncompatibleWith() {
      return inCompatibleIntegers;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      return ((InCompatibleInteger) other).value == value;
    }
  }

  private InCompatibleInteger one, two, three, four;

  @Before
  public void setUp() {
    one = new InCompatibleInteger(1, 2, 3);
    two = new InCompatibleInteger(2, 1);
    three = new InCompatibleInteger(3, 1);
    four = new InCompatibleInteger(4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowWithIncompatibleValues() {
    // When:
    new CompatibleSet<>(ImmutableSet.of(one, two));
  }

  @Test
  public void shouldReturnAllValues() {
    // Given:
    CompatibleSet<InCompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<InCompatibleInteger>of(one, four));

    // When:
    Set<InCompatibleInteger> all = compatibleSet.all();

    // Then:
    assertThat(all, hasSize(2));
    assertThat(all, containsInAnyOrder(one, four));
  }

  @Test
  public void shouldReturnCorrectForContainsWithEmptySet() {
    // Given:
    CompatibleSet<InCompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<InCompatibleInteger>of());

    // When:
    boolean containsOne = compatibleSet.contains(one);
    boolean containsTwo = compatibleSet.contains(two);
    boolean containsThree = compatibleSet.contains(three);
    boolean containsFour = compatibleSet.contains(four);

    // Then:
    assertFalse(containsOne);
    assertFalse(containsTwo);
    assertFalse(containsThree);
    assertFalse(containsFour);
  }

  @Test
  public void shouldReturnCorrectForContains() {
    // Given:
    CompatibleSet<InCompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.of(one, four));

    // When:
    boolean containsOne = compatibleSet.contains(one);
    boolean containsTwo = compatibleSet.contains(two);
    boolean containsFour = compatibleSet.contains(four);

    // Then:
    assertTrue(containsOne);
    assertFalse(containsTwo);
    assertTrue(containsFour);
  }

  @Test
  public void shouldReturnCorrectForFindAnyWithEmptySet() {
    // Given:
    CompatibleSet<InCompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<InCompatibleInteger>of());

    // When:
    Optional<InCompatibleInteger> containsOne = compatibleSet.findAny(ImmutableSet.of(one));
    Optional<InCompatibleInteger> containsTwo = compatibleSet.findAny(ImmutableSet.of(one, two));
    Optional<InCompatibleInteger> containsTree = compatibleSet.findAny(ImmutableSet.of(three, two));

    // Then:
    assertFalse(containsOne.isPresent());
    assertFalse(containsTwo.isPresent());
    assertFalse(containsTree.isPresent());
  }

  @Test
  public void shouldReturnCorrectForFindAny() {
    // Given:
    CompatibleSet<InCompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<InCompatibleInteger>of(two, three, four));

    // When:
    Optional<InCompatibleInteger> containsOne = compatibleSet.findAny(ImmutableSet.of(one));
    Optional<InCompatibleInteger> containsTwo = compatibleSet.findAny(ImmutableSet.of(one, two));
    Optional<InCompatibleInteger> containsTree = compatibleSet.findAny(ImmutableSet.of(three, two));

    // Then:
    assertFalse(containsOne.isPresent());
    assertTrue(containsTwo.isPresent());
    assertThat(containsTwo.get().value, is(2));
    assertTrue(containsTree.isPresent());
    assertThat(containsTree.get().value, is(3));
  }
}
