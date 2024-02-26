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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Set;

public class CompatibleSetTest {

  private static class CompatibleInteger implements CompatibleElement<CompatibleInteger> {

    final private Set<CompatibleInteger> CompatibleIntegers;
    final private int value;

    public CompatibleInteger(final int value) {
      this.value = value;
      CompatibleIntegers = ImmutableSet.of();
    }

    public CompatibleInteger(final int value, final Integer... CompatibleIntegers) {
      this.CompatibleIntegers = Arrays.stream(CompatibleIntegers)
          .map(CompatibleInteger::new)
          .collect(ImmutableSet.toImmutableSet());
      this.value = value;
    }

    public Set<CompatibleInteger> getIncompatibleWith() {
      return CompatibleIntegers;
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

      return ((CompatibleInteger) other).value == value;
    }
  }

  private CompatibleInteger one, two, three, four;

  @Before
  public void setUp() {
    one = new CompatibleInteger(1, 2, 3);
    two = new CompatibleInteger(2, 1);
    three = new CompatibleInteger(3, 1);
    four = new CompatibleInteger(4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowWithIncompatibleValues() {
    // When:
    new CompatibleSet<>(ImmutableSet.of(one, two));
  }

  @Test
  public void shouldReturnAllValues() {
    // Given:
    CompatibleSet<CompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<CompatibleInteger>of(one, four));

    // When:
    Set<CompatibleInteger> all = compatibleSet.all();

    // Then:
    assertThat(all, containsInAnyOrder(one, four));
  }

  @Test
  public void shouldReturnCorrectForContainsWithEmptySet() {
    // Given:
    CompatibleSet<CompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<CompatibleInteger>of());

    // When: Then:
    assertThat(compatibleSet.contains(one), is(false));
    assertThat(compatibleSet.contains(two), is(false));
    assertThat(compatibleSet.contains(three), is(false));
    assertThat(compatibleSet.contains(four), is(false));
  }

  @Test
  public void shouldReturnCorrectForContains() {
    // Given:
    CompatibleSet<CompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.of(one, four));

    // When: Then:
    assertThat(compatibleSet.contains(one), is(true));
    assertThat(compatibleSet.contains(two), is(false));
    assertThat(compatibleSet.contains(four), is(true));
  }

  @Test
  public void shouldReturnCorrectForFindAnyWithEmptySet() {
    // Given:
    CompatibleSet<CompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<CompatibleInteger>of());

    // When:
    Optional<CompatibleInteger> containsOne = compatibleSet.findAny(ImmutableSet.of(one));
    Optional<CompatibleInteger> containsTwo = compatibleSet.findAny(ImmutableSet.of(one, two));
    Optional<CompatibleInteger> containsTree = compatibleSet.findAny(ImmutableSet.of(three, two));

    // Then:
    assertFalse(containsOne.isPresent());
    assertFalse(containsTwo.isPresent());
    assertFalse(containsTree.isPresent());
  }

  @Test
  public void shouldReturnCorrectForFindAny() {
    // Given:
    CompatibleSet<CompatibleInteger> compatibleSet = new CompatibleSet<>(
        ImmutableSet.<CompatibleInteger>of(two, three, four));

    // When:
    Optional<CompatibleInteger> containsOne = compatibleSet.findAny(ImmutableSet.of(one));
    Optional<CompatibleInteger> containsTwo = compatibleSet.findAny(ImmutableSet.of(one, two));
    Optional<CompatibleInteger> containsTree = compatibleSet.findAny(ImmutableSet.of(three, two));

    // Then:
    assertFalse(containsOne.isPresent());
    assertTrue(containsTwo.isPresent());
    assertThat(containsTwo.get().value, is(2));
    assertTrue(containsTree.isPresent());
    assertThat(containsTree.get().value, is(3));
  }
}
