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

package io.confluent.ksql.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.GrammaticalJoiner;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


/**
 * Validated set of enabled features
 *
 * <p>Known to not have conflicting features enabled
 */
@Immutable
public final class SerdeFeatures {

  public static final ImmutableSet<SerdeFeature> WRAPPING_FEATURES = ImmutableSet.of(
      SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES
  );

  private final ImmutableSet<SerdeFeature> features;

  @JsonCreator
  public static SerdeFeatures from(final Set<SerdeFeature> features) {
    return new SerdeFeatures(features);
  }

  public static SerdeFeatures of(final SerdeFeature... features) {
    return new SerdeFeatures(ImmutableSet.copyOf(features));
  }

  private SerdeFeatures(final Set<SerdeFeature> features) {
    validate(features);
    this.features = ImmutableSet.copyOf(features);
  }

  public boolean enabled(final SerdeFeature feature) {
    return features.contains(feature);
  }

  @JsonValue
  public Set<SerdeFeature> all() {
    return features;
  }

  public Optional<SerdeFeature> findAny(final Set<SerdeFeature> anyOf) {
    return anyOf.stream()
        .filter(features::contains)
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
    final SerdeFeatures that = (SerdeFeatures) o;
    return Objects.equals(features, that.features);
  }

  @Override
  public int hashCode() {
    return Objects.hash(features);
  }

  @Override
  public String toString() {
    return features.toString();
  }

  private static void validate(final Set<SerdeFeature> features) {
    features.forEach(f -> {
      final Set<SerdeFeature> incompatible = Sets.intersection(f.getIncompatibleWith(), features);
      if (!incompatible.isEmpty()) {
        throw new IllegalArgumentException("Can't set "
            + f + " with " + GrammaticalJoiner.or().join(incompatible));
      }
    });
  }

  /**
   * Custom Jackson content filter that excludes empty {@link SerdeFeatures}.
   */
  @SuppressWarnings({"checkstyle:TypeName", "checkstyle:AbbreviationAsWordInName"})
  public static final class NOT_EMPTY {

    @SuppressFBWarnings({"EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS", "EQ_UNUSUAL"})
    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof SerdeFeatures)) {
        return false;
      }

      return ((SerdeFeatures) obj).features.isEmpty();
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
