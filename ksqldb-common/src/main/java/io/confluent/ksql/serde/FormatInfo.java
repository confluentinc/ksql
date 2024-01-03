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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Immutable Pojo holding standard info about a format.
 */
@Immutable
public final class FormatInfo {

  private final String format;
  private final ImmutableMap<String, String> properties;

  public static FormatInfo of(final String format) {
    return FormatInfo.of(format, ImmutableMap.of());
  }

  @JsonCreator
  public static FormatInfo of(
      @JsonProperty(value = "format", required = true) final String format,
      @JsonProperty(value = "properties") final Optional<Map<String, String>> properties
  ) {
    return new FormatInfo(format, properties.orElse(ImmutableMap.of()));
  }

  public static FormatInfo of(
      final String format,
      final Map<String, String> properties
  ) {
    return new FormatInfo(format, properties);
  }

  private FormatInfo(final String format, final Map<String, String> properties) {
    this.format = Objects.requireNonNull(format, "format").toUpperCase();
    this.properties = ImmutableMap.copyOf(Objects.requireNonNull(properties, "properties"));
  }

  public String getFormat() {
    return format;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "properties is ImmutableMap")
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final FormatInfo that = (FormatInfo) o;
    return Objects.equals(format, that.format)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, properties);
  }

  @Override
  public String toString() {
    return "FormatInfo{"
        + "format=" + format
        + ", properties=" + properties
        + '}';
  }

  private ImmutableMap<String, String> getPropertiesWithoutProperty(final String propName) {
    final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();

    return propertiesBuilder.putAll(
        this
            .getProperties()
            .entrySet()
            .stream()
            .filter(x -> !x.getKey().equals(propName))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    ).build();
  }

  public FormatInfo copyWithoutProperty(final String propName) {
    return FormatInfo.of(this.getFormat(), getPropertiesWithoutProperty(propName));
  }

}