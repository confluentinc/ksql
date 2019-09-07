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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.model.WindowType;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable Pojo holding information about a source's key format.
 */
@Immutable
public final class KeyFormat {
  private static final String FORMAT_INFO = "formatInfo";
  private static final String WINDOW_INFO = "windowInfo";

  @JsonProperty(FORMAT_INFO)
  private final FormatInfo format;
  @JsonProperty(WINDOW_INFO)
  private final Optional<WindowInfo> window;

  public static KeyFormat nonWindowed(final FormatInfo format) {
    return new KeyFormat(format, Optional.empty());
  }

  public static KeyFormat windowed(
      final Format format,
      final Optional<String> avroSchemaName,
      final WindowInfo windowInfo
  ) {
    return new KeyFormat(
        FormatInfo.of(format, avroSchemaName),
        Optional.of(windowInfo)
    );
  }

  public static KeyFormat windowed(
      final FormatInfo format,
      final WindowInfo windowInfo
  ) {
    return new KeyFormat(format, Optional.of(windowInfo));
  }

  @JsonCreator
  private KeyFormat(
      @JsonProperty(FORMAT_INFO) final FormatInfo format,
      @JsonProperty(WINDOW_INFO) final Optional<WindowInfo> window
  ) {
    this.format = Objects.requireNonNull(format, "format");
    this.window = Objects.requireNonNull(window, "window");
  }

  public Format getFormat() {
    return format.getFormat();
  }

  public FormatInfo getFormatInfo() {
    return format;
  }

  public boolean isWindowed() {
    return window.isPresent();
  }

  public Optional<WindowInfo> getWindowInfo() {
    return window;
  }

  public Optional<WindowType> getWindowType() {
    return window.map(WindowInfo::getType);
  }

  public Optional<Duration> getWindowSize() {
    return window.flatMap(WindowInfo::getSize);
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
        && Objects.equals(window, keyFormat.window);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, window);
  }

  @Override
  public String toString() {
    return "KeyFormat{"
        + "format=" + format
        + ", window=" + window
        + '}';
  }

}
