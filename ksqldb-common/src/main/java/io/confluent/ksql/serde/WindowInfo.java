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
import io.confluent.ksql.parser.OutputRefinement;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable pojo for storing info about a window.
 */
@Immutable
public final class WindowInfo {

  private final WindowType type;
  private final Optional<Duration> size;
  private final OutputRefinement emitStrategy;

  @JsonCreator
  public static WindowInfo of(
      @JsonProperty(value = "type", required = true) final WindowType type,
      @JsonProperty(value = "size") final Optional<Duration> size,
      @JsonProperty(value = "emitStrategy") final Optional<OutputRefinement> emitStrategy) {
    return new WindowInfo(type, size, emitStrategy);
  }

  private WindowInfo(
      final WindowType type,
      final Optional<Duration> size,
      final Optional<OutputRefinement> emitStrategy
  ) {
    this.type = Objects.requireNonNull(type, "type");
    this.size = Objects.requireNonNull(size, "size");
    this.emitStrategy = Objects.requireNonNull(
        emitStrategy.orElse(OutputRefinement.CHANGES),
        "emitStrategy");

    if (type.requiresWindowSize() && !size.isPresent()) {
      throw new IllegalArgumentException("Size required");
    }

    if (!type.requiresWindowSize() && size.isPresent()) {
      throw new IllegalArgumentException("Size not required");
    }

    if (size.isPresent() && (size.get().isZero() || size.get().isNegative())) {
      throw new IllegalArgumentException("Size must be positive");
    }
  }

  public WindowType getType() {
    return type;
  }

  public Optional<Duration> getSize() {
    return size;
  }

  public OutputRefinement getEmitStrategy() {
    return emitStrategy;
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

    // we omit `emitStrategy` because `WindowInfo` is used to determine the topic format,
    // and the emit-strategy has no impact on the serialization format
    return type == that.type
        && Objects.equals(size, that.size);
  }

  @Override
  public int hashCode() {
    // we omit `emitStrategy` because `WindowInfo` is used to determine the topic format,
    // and the emit-strategy has no impact on the serialization format
    return Objects.hash(type, size);
  }

  @Override
  public String toString() {
    return "WindowInfo{"
        + "type=" + type
        + ", size=" + size.map(Duration::toMillis)
        + ", emitStrategy=" + emitStrategy
        + '}';
  }
}
