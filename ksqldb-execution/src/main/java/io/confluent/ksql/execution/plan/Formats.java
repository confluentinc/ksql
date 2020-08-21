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

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Immutable
public final class Formats {

  private final FormatInfo keyFormat;
  private final FormatInfo valueFormat;
  private final SerdeOptions options;

  public static Formats of(
      final FormatInfo keyFormat,
      final FormatInfo valueFormat,
      final SerdeOptions options
  ) {
    return new Formats(keyFormat, valueFormat, options);
  }

  public static Formats of(
      final KeyFormat keyFormat,
      final ValueFormat valueFormat,
      final SerdeOptions options
  ) {
    return of(keyFormat.getFormatInfo(), valueFormat.getFormatInfo(), options);
  }

  @JsonCreator
  public static Formats from(
      @JsonProperty(value = "keyFormat", required = true) final FormatInfo keyFormat,
      @JsonProperty(value = "valueFormat", required = true) final FormatInfo valueFormat,
      @JsonProperty(value = "options") final Optional<Set<SerdeOption>> options
  ) {
    final SerdeOptions validatedOptions = SerdeOptions.of(
        Objects.requireNonNull(options, "options")
            .map(ImmutableSet::copyOf)
            .orElseGet(ImmutableSet::of)
    );

    return Formats.of(keyFormat, valueFormat, validatedOptions);
  }

  private Formats(
      final FormatInfo keyFormat,
      final FormatInfo valueFormat,
      final SerdeOptions options
  ) {
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.options = Objects.requireNonNull(options, "options");
  }

  public FormatInfo getKeyFormat() {
    return keyFormat;
  }

  public FormatInfo getValueFormat() {
    return valueFormat;
  }

  @JsonIgnore
  public SerdeOptions getOptions() {
    return options;
  }

  @SuppressWarnings("unused") // Invoked by reflection by Jackson.
  @JsonProperty("options")
  public Set<SerdeOption> getOptionsSet() {
    return options.all();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Formats formats = (Formats) o;
    return Objects.equals(keyFormat, formats.keyFormat)
        && Objects.equals(valueFormat, formats.valueFormat)
        && Objects.equals(options, formats.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyFormat, valueFormat, options);
  }
}
