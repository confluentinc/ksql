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
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validated set of {@link SerdeOption}s.
 *
 * <p>The class ensures no invalid combinations of options are possible.
 */
@Immutable
public final class SerdeOptions {

  private static final ImmutableSet<SerdeOption> KEY_WRAPPING_OPTIONS = ImmutableSet.of(
      SerdeOption.UNWRAP_SINGLE_KEYS
  );

  private static final ImmutableSet<SerdeOption> VALUE_WRAPPING_OPTIONS = ImmutableSet.of(
      SerdeOption.WRAP_SINGLE_VALUES, SerdeOption.UNWRAP_SINGLE_VALUES
  );

  private final ImmutableSet<SerdeOption> options;

  public static SerdeOptions of(final SerdeOption... options) {
    return new SerdeOptions(ImmutableSet.copyOf(options));
  }

  public static SerdeOptions of(final Set<SerdeOption> options) {
    return new SerdeOptions(ImmutableSet.copyOf(options));
  }

  public static Builder builder() {
    return new Builder();
  }

  private SerdeOptions(final ImmutableSet<SerdeOption> options) {
    this.options = Objects.requireNonNull(options, "options");
    validate(this.options);
  }

  public EnabledSerdeFeatures keyFeatures() {
    return features(true);
  }

  public EnabledSerdeFeatures valueFeatures() {
    return features(false);
  }

  public Set<SerdeOption> all() {
    return options;
  }

  public Optional<SerdeOption> keyWrapping() {
    return Optional.ofNullable(
        Iterables.getFirst(Sets.intersection(options, KEY_WRAPPING_OPTIONS), null)
    );
  }

  public Optional<SerdeOption> valueWrapping() {
    return Optional.ofNullable(
        Iterables.getFirst(Sets.intersection(options, VALUE_WRAPPING_OPTIONS), null)
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
    return options.toString();
  }

  private EnabledSerdeFeatures features(final boolean key) {
    return EnabledSerdeFeatures.from(options.stream()
        .filter(option -> option.isKeyOption() == key)
        .map(SerdeOption::requiredFeature)
        .collect(Collectors.toSet()));
  }

  private static void validate(final Set<SerdeOption> options) {
    final Set<SerdeOption> wrappingOptions = Sets.intersection(options, VALUE_WRAPPING_OPTIONS);
    if (wrappingOptions.size() > 1) {
      throw new IllegalArgumentException("Conflicting wrapping settings: " + options);
    }
  }

  public static final class Builder {

    private final Set<SerdeOption> options = EnumSet.noneOf(SerdeOption.class);

    public Builder add(final SerdeOption option) {
      if (options.add(option)) {
        try {
          validate(options);
        } catch (final Exception e) {
          options.remove(option);
          throw e;
        }
      }
      return this;
    }

    public SerdeOptions build() {
      return SerdeOptions.of(options);
    }
  }
}
