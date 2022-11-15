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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Immutable
public final class Formats {

  private enum LegacyOption {
    WRAP_SINGLE_VALUES,
    UNWRAP_SINGLE_VALUES
  }

  private final FormatInfo keyFormat;
  private final FormatInfo valueFormat;
  private final SerdeFeatures keyFeatures;
  private final SerdeFeatures valueFeatures;

  public static Formats of(
      final FormatInfo keyFormat,
      final FormatInfo valueFormat,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valFeatures
  ) {
    return new Formats(keyFormat, valueFormat, keyFeatures, valFeatures);
  }

  public static Formats from(final KsqlTopic topic) {
    return of(
        topic.getKeyFormat().getFormatInfo(),
        topic.getValueFormat().getFormatInfo(),
        topic.getKeyFormat().getFeatures(),
        topic.getValueFormat().getFeatures()
    );
  }

  /**
   * Factory method used by Jackson.
   *
   * <p>Supports deserializing the valueFeatures from a legacy {@code options} field to support
   * older plans (pre 0.13).
   */
  @SuppressWarnings("unused") // Invoked by reflection by Jackson
  @JsonCreator
  static Formats from(
      @JsonProperty(value = "keyFormat", required = true) final FormatInfo keyFormat,
      @JsonProperty(value = "valueFormat", required = true) final FormatInfo valueFormat,
      @JsonProperty(value = "keyFeatures") final Optional<SerdeFeatures> keyFeatures,
      @JsonProperty(value = "valueFeatures") final Optional<SerdeFeatures> valueFeatures,
      @JsonProperty(value = "options") final Optional<Set<LegacyOption>> legacyOptions
  ) {
    return Formats.of(
        keyFormat,
        valueFormat,
        keyFeatures.orElseGet(SerdeFeatures::of),
        valueFeatures.orElseGet(() -> handleLegacy(legacyOptions))
    );
  }

  private Formats(
      final FormatInfo keyFormat,
      final FormatInfo valueFormat,
      final SerdeFeatures keyFeatures,
      final SerdeFeatures valueFeatures
  ) {
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.keyFeatures = Objects.requireNonNull(keyFeatures, "keyFeatures");
    this.valueFeatures = Objects.requireNonNull(valueFeatures, "valueFeatures");
  }

  public FormatInfo getKeyFormat() {
    return keyFormat;
  }

  public FormatInfo getValueFormat() {
    return valueFormat;
  }

  @JsonInclude(value = Include.CUSTOM, valueFilter = SerdeFeatures.NOT_EMPTY.class)
  public SerdeFeatures getKeyFeatures() {
    return keyFeatures;
  }

  @JsonInclude(value = Include.CUSTOM, valueFilter = SerdeFeatures.NOT_EMPTY.class)
  public SerdeFeatures getValueFeatures() {
    return valueFeatures;
  }

  /**
   * Required to ensure the JSON schema of the command topic messages continues to contain the
   * legacy {@code options} field.
   */
  @SuppressWarnings("MethodMayBeStatic")
  @Deprecated
  @JsonInclude(Include.NON_NULL)
  public Set<LegacyOption> getOptions() {
    return null;
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
        && Objects.equals(keyFeatures, formats.keyFeatures)
        && Objects.equals(valueFeatures, formats.valueFeatures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyFormat, valueFormat, keyFeatures, valueFeatures);
  }

  private static SerdeFeatures handleLegacy(
      final Optional<Set<LegacyOption>> possibleLegacyOptions
  ) {
    if (!possibleLegacyOptions.isPresent()) {
      return SerdeFeatures.of();
    }

    final Set<LegacyOption> legacyOptions = possibleLegacyOptions.get();
    if (legacyOptions.isEmpty()) {
      return SerdeFeatures.of();
    }

    if (legacyOptions.size() != 1) {
      throw new IllegalArgumentException("Invalid legacy options: " + legacyOptions);
    }

    return legacyOptions.iterator().next() == LegacyOption.WRAP_SINGLE_VALUES
        ? SerdeFeatures.of(SerdeFeature.WRAP_SINGLES)
        : SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES);
  }
}
