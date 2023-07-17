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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.model.WindowType;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable Pojo holding information about a source's key format.
 */
@Immutable
public final class KeyFormat {

  private final FormatInfo format;
  private final SerdeFeatures features;
  private final Optional<WindowInfo> window;

  public static KeyFormat nonWindowed(
      final FormatInfo format,
      final SerdeFeatures features
  ) {
    return new KeyFormat(format, features, Optional.empty());
  }

  public static KeyFormat windowed(
      final FormatInfo format,
      final SerdeFeatures features,
      final WindowInfo windowInfo
  ) {
    return new KeyFormat(format, features, Optional.of(windowInfo));
  }

  public static KeyFormat of(
      final FormatInfo format,
      final SerdeFeatures features,
      final Optional<WindowInfo> windowInfo
  ) {
    return new KeyFormat(format, features, windowInfo);
  }

  @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD")
  @SuppressWarnings("unused") // Invoked via reflection by Jackson
  @JsonCreator
  private static KeyFormat of(
      @JsonProperty(value = "format", required = true) final String format,
      @JsonProperty(value = "properties") final Optional<Map<String, String>> properties,
      @JsonProperty(value = "features") final Optional<SerdeFeatures> features,
      @JsonProperty(value = "windowInfo") final Optional<WindowInfo> windowInfo
  ) {
    return new KeyFormat(
        FormatInfo.of(format, properties.orElseGet(ImmutableMap::of)),
        features.orElseGet(SerdeFeatures::of),
        windowInfo
    );
  }

  private KeyFormat(
      final FormatInfo format,
      final SerdeFeatures features,
      final Optional<WindowInfo> window
  ) {
    this.format = Objects.requireNonNull(format, "format");
    this.features = Objects.requireNonNull(features, "features");
    this.window = Objects.requireNonNull(window, "window");
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

  @JsonIgnore
  public boolean isWindowed() {
    return window.isPresent();
  }

  public Optional<WindowInfo> getWindowInfo() {
    return window;
  }

  @JsonIgnore
  public Optional<WindowType> getWindowType() {
    return window.map(WindowInfo::getType);
  }

  @JsonIgnore
  public Optional<Duration> getWindowSize() {
    return window.flatMap(WindowInfo::getSize);
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
    final KeyFormat keyFormat = (KeyFormat) o;
    return Objects.equals(format, keyFormat.format)
        && Objects.equals(features, keyFormat.features)
        && Objects.equals(window, keyFormat.window);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, features, window);
  }

  @Override
  public String toString() {
    return "KeyFormat{"
        + "format=" + format
        + ", features=" + features
        + ", window=" + window
        + '}';
  }

  public KeyFormat withSerdeFeatures(final SerdeFeatures additionalFeatures) {
    final HashSet<SerdeFeature> features = new HashSet<>(this.features.all());
    features.addAll(additionalFeatures.all());
    return new KeyFormat(
        format,
        SerdeFeatures.from(features),
        window
    );
  }

  public KeyFormat withoutSerdeFeatures(final SerdeFeatures featuresToRemove) {
    final HashSet<SerdeFeature> features = new HashSet<>(this.features.all());
    features.removeAll(featuresToRemove.all());
    return new KeyFormat(
        format,
        SerdeFeatures.from(features),
        window
    );
  }
}
