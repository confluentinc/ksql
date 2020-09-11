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

import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.Set;


/**
 * Validated set of enabled features
 *
 * <p>Known to not have conflicting features enabled
 */
@Immutable
public final class EnabledSerdeFeatures {

  private final ImmutableSet<SerdeFeature> features;

  @JsonCreator
  public static EnabledSerdeFeatures from(final Set<SerdeFeature> features) {
    return new EnabledSerdeFeatures(features);
  }

  public static EnabledSerdeFeatures of(final SerdeFeature... features) {
    return new EnabledSerdeFeatures(ImmutableSet.copyOf(features));
  }

  private EnabledSerdeFeatures(final Set<SerdeFeature> features) {
    validate(features);
    this.features = ImmutableSet.copyOf(features);
  }

  public boolean enabled(final SerdeFeature feature) {
    return features.contains(feature);
  }

  public Set<SerdeFeature> all() {
    return features;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EnabledSerdeFeatures that = (EnabledSerdeFeatures) o;
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
    if (features.contains(WRAP_SINGLES) && features.contains(UNWRAP_SINGLES)) {
      throw new IllegalArgumentException("Can't set both "
          + WRAP_SINGLES + " and " + UNWRAP_SINGLES);
    }
  }
}
