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

package io.confluent.ksql.serde;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Validated set of {@link SerdeOption}s.
 *
 * <p>The class ensures no invalid combinations of options are possible.
 */
@Immutable
public final class SerdeOptions {

  private static final ImmutableSet<SerdeOption> WRAPPING_OPTIONS = ImmutableSet.of(
      SerdeOption.WRAP_SINGLE_VALUES, SerdeOption.UNWRAP_SINGLE_VALUES
  );

  private final ImmutableSet<SerdeOption> options;

  public static SerdeOptions of(final SerdeOption... options) {
    return new SerdeOptions(ImmutableSet.copyOf(options));
  }

  public static SerdeOptions of(final Set<SerdeOption> options) {
    return new SerdeOptions(ImmutableSet.copyOf(options));
  }

  private SerdeOptions(final ImmutableSet<SerdeOption> options) {
    this.options = validate(Objects.requireNonNull(options, "options"));
  }

  public Set<SerdeOption> all() {
    return options;
  }

  public Optional<SerdeOption> valueWrapping() {
    return Optional.ofNullable(
        Iterables.getFirst(Sets.intersection(options, WRAPPING_OPTIONS), null)
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SerdeOptions that = (SerdeOptions) o;
    return Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(options);
  }

  @Override
  public String toString() {
    return "SerdeOptions" + options;
  }

  private static ImmutableSet<SerdeOption> validate(final ImmutableSet<SerdeOption> options) {
    final Set<SerdeOption> wrappingOptions = Sets.intersection(options, WRAPPING_OPTIONS);
    if (wrappingOptions.size() > 1) {
      throw new IllegalArgumentException("Conflicting wrapping settings: " + options);
    }

    return options;
  }
}
