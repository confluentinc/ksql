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

import static org.hamcrest.Matchers.allOf;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatMatchers.KeyFormatMatchers;
import io.confluent.ksql.serde.KeyFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

public class KeyFormatNode {

  private final Optional<Format> format;
  private final Optional<WindowType> windowType;
  private final Optional<Duration> windowSize;

  public KeyFormatNode(
      @JsonProperty("format") final Format format,
      @JsonProperty("windowType") final WindowType windowType,
      @JsonProperty("windowSize") final Long windowSize
  ) {
    this.format = Optional.ofNullable(format);
    this.windowType = Optional.ofNullable(windowType);
    this.windowSize = Optional.ofNullable(windowSize)
        .map(Duration::ofMillis);
  }

  @SuppressWarnings("unchecked")
  Matcher<? super KeyFormat> build() {
    final Matcher<KeyFormat> formatMatcher = format
        .map(Matchers::is)
        .map(KeyFormatMatchers::hasFormat)
        .orElse(null);

    final Matcher<KeyFormat> windowTypeMatcher = KeyFormatMatchers
        .hasWindowType(Matchers.is(windowType));

    final Matcher<KeyFormat> windowSizeMatcher = KeyFormatMatchers
        .hasWindowSize(Matchers.is(windowSize));

    final Matcher<KeyFormat>[] matchers = Stream
        .of(formatMatcher, windowTypeMatcher, windowSizeMatcher)
        .filter(Objects::nonNull)
        .toArray(Matcher[]::new);

    return allOf(matchers);
  }
}
