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
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.util.CompatibleSet;
import java.util.Set;


/**
 * Validated set of enabled Serde features
 *
 * <p>Known to not have conflicting features enabled
 */
@Immutable
public final class SerdeFeatures extends CompatibleSet<SerdeFeature> {

  public static final ImmutableSet<SerdeFeature> WRAPPING_FEATURES = ImmutableSet.of(
      SerdeFeature.WRAP_SINGLES, SerdeFeature.UNWRAP_SINGLES
  );

  @JsonCreator
  public static SerdeFeatures from(final Set<SerdeFeature> features) {
    return new SerdeFeatures(features);
  }

  public static SerdeFeatures of(final SerdeFeature... features) {
    return new SerdeFeatures(ImmutableSet.copyOf(features));
  }

  private SerdeFeatures(final Set<SerdeFeature> features) {
    super(features);
  }

  public boolean enabled(final SerdeFeature feature) {
    return contains(feature);
  }

  @JsonValue
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "incompatibleWith is ImmutableSet")
  @Override
  public Set<SerdeFeature> all() {
    return values;
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

      return ((SerdeFeatures) obj).values.isEmpty();
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }
}
