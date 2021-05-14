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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.test.tools.VersionBounds;
import java.util.Optional;

/**
 * JSON serializable Pojo controlling the version bounds a test if valid for
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VersionBoundsNode {

  private final Optional<KsqlVersion> min;
  private final Optional<KsqlVersion> max;

  @SuppressWarnings("WeakerAccess") // Invoked via reflection
  public VersionBoundsNode(
      @JsonProperty("min") final Optional<String> min,
      @JsonProperty("max") final Optional<String> max
  ) {
    this.min = requireNonNull(min, "min").map(KsqlVersion::parse);
    this.max = requireNonNull(max, "max").map(KsqlVersion::parse);
  }

  static VersionBoundsNode allVersions() {
    return new VersionBoundsNode(Optional.empty(), Optional.empty());
  }

  public VersionBounds build() {
    return VersionBounds.of(min, max);
  }
}
