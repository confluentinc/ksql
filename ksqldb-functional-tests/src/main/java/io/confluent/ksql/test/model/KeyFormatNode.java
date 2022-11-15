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

package io.confluent.ksql.test.model;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.allOf;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.test.model.matchers.FormatMatchers.KeyFormatMatchers;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

public final class KeyFormatNode {

  private final Optional<String> format;
  private final Optional<WindowType> windowType;
  private final Optional<Long> windowSize;

  public KeyFormatNode(
      @JsonProperty("format") final Optional<String> format,
      @JsonProperty("windowType") final Optional<WindowType> windowType,
      @JsonProperty("windowSize") final Optional<Long> windowSize
  ) {
    this.format = requireNonNull(format, "format");
    this.windowType = requireNonNull(windowType);
    this.windowSize = requireNonNull(windowSize);

    // Validate and throw early on bad data:
    build();
  }

  @SuppressWarnings("unused") // Invoked via reflection
  public Optional<String> getFormat() {
    return format;
  }

  @SuppressWarnings("unused") // Invoked via reflection
  public Optional<WindowType> getWindowType() {
    return windowType;
  }

  @SuppressWarnings("unused") // Invoked via reflection
  public Optional<Long> getWindowSize() {
    return windowSize;
  }

  @SuppressWarnings("unchecked")
  Matcher<? super KeyFormat> build() {
    final Matcher<KeyFormat> formatMatcher = format
        .map(FormatInfo::of)
        .map(FormatFactory::of)
        .map(Matchers::is)
        .map(KeyFormatMatchers::hasFormat)
        .orElse(null);

    final Matcher<KeyFormat> windowTypeMatcher = KeyFormatMatchers
        .hasWindowType(Matchers.is(windowType));

    final Matcher<KeyFormat> windowSizeMatcher = KeyFormatMatchers
        .hasWindowSize(Matchers.is(windowSize.map(Duration::ofMillis)));

    final Matcher<KeyFormat>[] matchers = Stream
        .of(formatMatcher, windowTypeMatcher, windowSizeMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KeyFormatNode that = (KeyFormatNode) o;
    return format.equals(that.format)
        && windowType.equals(that.windowType)
        && windowSize.equals(that.windowSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, windowType, windowSize);
  }

  public static KeyFormatNode fromKeyFormat(final KeyFormat keyFormat) {
    return new KeyFormatNode(
        Optional.of(keyFormat.getFormatInfo().getFormat()),
        keyFormat.getWindowType(),
        keyFormat.getWindowSize().map(Duration::toMillis)
    );
  }
}
