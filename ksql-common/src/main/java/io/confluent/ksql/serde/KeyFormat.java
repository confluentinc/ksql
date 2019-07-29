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

  private final FormatInfo format;
  private final Optional<WindowInfo> window;

  public static KeyFormat nonWindowed(
      final Format format
  ) {
    return new KeyFormat(
        FormatInfo.of(format, Optional.empty()),
        Optional.empty()
    );
  }

  public static KeyFormat nonWindowed(
      final Format format,
      final Optional<String> avroSchemaName
  ) {
    return new KeyFormat(
        FormatInfo.of(format, avroSchemaName),
        Optional.empty()
    );
  }

  public static KeyFormat windowed(
      final Format format,
      final WindowType windowType,
      final Optional<Duration> windowSize
  ) {
    return new KeyFormat(
        FormatInfo.of(format, Optional.empty()),
        Optional.of(new WindowInfo(windowType, windowSize))
    );
  }

  public static KeyFormat windowed(
      final Format format,
      final Optional<String> avroSchemaName,
      final WindowType windowType,
      final Optional<Duration> windowSize
  ) {
    return new KeyFormat(
        FormatInfo.of(format, avroSchemaName),
        Optional.of(new WindowInfo(windowType, windowSize))
    );
  }

  private KeyFormat(
      final FormatInfo format,
      final Optional<WindowInfo> window
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

  private static final class WindowInfo {

    private final WindowType type;
    private final Optional<Duration> size;

    private WindowInfo(final WindowType type, final Optional<Duration> size) {
      this.type = Objects.requireNonNull(type, "type");
      this.size = Objects.requireNonNull(size, "size");

      if (type.requiresWindowSize() && !size.isPresent()) {
        throw new IllegalArgumentException("Size required");
      }

      if (!type.requiresWindowSize() && size.isPresent()) {
        throw new IllegalArgumentException("Size not required");
      }
    }

    public WindowType getType() {
      return type;
    }

    public Optional<Duration> getSize() {
      return size;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final WindowInfo that = (WindowInfo) o;
      return type == that.type
          && Objects.equals(size, that.size);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, size);
    }

    @Override
    public String toString() {
      return "WindowInfo{"
          + "type=" + type
          + ", size=" + size.map(Duration::toMillis)
          + '}';
    }
  }
}
