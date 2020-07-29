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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.OutputRefinement;
import java.util.Objects;

/**
 * Immutable pojo for storing info about a refinement.
 */
@Immutable
public final class RefinementInfo {

  private final OutputRefinement outputRefinement;

  @JsonCreator
  public static RefinementInfo of(
      @JsonProperty(value = "outputRefinement", required = true)
      final OutputRefinement outputRefinement
  ) {
    return new RefinementInfo(outputRefinement);
  }

  private RefinementInfo(final OutputRefinement outputRefinement) {
    this.outputRefinement = Objects.requireNonNull(outputRefinement, "outputRefinement");
  }

  public OutputRefinement getOutputRefinement() {
    return outputRefinement;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RefinementInfo that = (RefinementInfo) o;
    return Objects.equals(outputRefinement, that.outputRefinement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(outputRefinement);
  }

  @Override
  public String toString() {
    return "RefinementInfo{"
        + "outputRefinement=" + outputRefinement
        + '}';
  }
}
