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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable Pojo holding information about a source's value format.
 */
@Immutable
public final class ValueFormat {

  private final FormatInfo format;
  private final SerdeFeatures features;

  public static ValueFormat of(
      final FormatInfo format,
      final SerdeFeatures features
  ) {
    return new ValueFormat(format, features);
  }

  @SuppressWarnings("unused") // Invoked via reflection by Jackson
  @JsonCreator
  private static ValueFormat create(
      @JsonProperty(value = "format", required = true) final String format,
      @JsonProperty(value = "properties") final Optional<Map<String, String>> properties,
      @JsonProperty(value = "features") final Optional<SerdeFeatures> features
  ) {
    return new ValueFormat(
        FormatInfo.of(format, properties.orElseGet(ImmutableMap::of)),
        features.orElseGet(SerdeFeatures::of)
    );
  }

  private ValueFormat(final FormatInfo format, final SerdeFeatures features) {
    this.format = Objects.requireNonNull(format, "format");
    this.features = Objects.requireNonNull(features, "features");
  }

  public String getFormat() {
    return format.getFormat();
  }

  public Map<String, String> getProperties() {
    return format.getProperties();
  }

  @JsonIgnore
  public FormatInfo getFormatInfo() {
    return format;
  }

  @JsonInclude(value = Include.CUSTOM, valueFilter = SerdeFeatures.NOT_EMPTY.class)
  public SerdeFeatures getFeatures() {
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
    final ValueFormat that = (ValueFormat) o;
    return Objects.equals(format, that.format)
        && Objects.equals(features, that.features);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, features);
  }

  @Override
  public String toString() {
    return "ValueFormat{"
        + "format=" + format
        + ", features=" + features
        + '}';
  }
}
