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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


/**
 * Validated set of compatible set elements
 *
 * <p>Known to not have conflicting elements
 */
@Immutable
public class CompatibleSet<T extends CompatibleElement<T>> {

  protected final ImmutableSet<T> values;

  public CompatibleSet(final Set<T> values) {
    validate(values);
    this.values = ImmutableSet.copyOf(values);
  }

  public boolean contains(final T value) {
    return values.contains(value);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "values is ImmutableSet")
  public Set<T> all() {
    return values;
  }

  public Optional<T> findAny(final Set<T> anyOf) {
    return anyOf.stream()
        .filter(values::contains)
        .findAny();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CompatibleSet<?> that = (CompatibleSet<?>) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public String toString() {
    return values.toString();
  }

  private static <T extends CompatibleElement<T>> void validate(final Set<T> values) {
    values.forEach(f -> {
      final Set<T> incompatible = Sets.intersection(f.getIncompatibleWith(), values);
      if (!incompatible.isEmpty()) {
        throw new IllegalArgumentException("Can't set "
            + f + " with " + GrammaticalJoiner.or().join(incompatible));
      }
    });
  }
}