/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * An object which joins pieces of text, with a separator and additional final word, allowing more
 * grammatically correct joining, e,g. "a, b, c or d".
 */
public final class GrammaticalJoiner {

  private final String separator;
  private final String lastSeparator;

  /**
   * @return a joiner with a comma separator and ' or ' as the final separator.
   */
  public static GrammaticalJoiner or() {
    return new GrammaticalJoiner(", ", " or ");
  }

  /**
   * @return a joiner with a comma separator and ' and ' as the final separator.
   */
  public static GrammaticalJoiner and() {
    return new GrammaticalJoiner(", ", " and ");
  }

  public static GrammaticalJoiner comma() {
    return new GrammaticalJoiner(", ", ", ");
  }

  private GrammaticalJoiner(final String separator, final String lastSeparator) {
    this.separator = requireNonNull(separator, "separator");
    this.lastSeparator = requireNonNull(lastSeparator, "lastSeparator");
  }

  public String join(final Stream<?> parts) {
    return join(parts.iterator());
  }

  public String join(final Iterable<?> parts) {
    return join(parts.iterator());
  }

  private String join(final Iterator<?> parts) {
    if (!parts.hasNext()) {
      return "";
    }

    String next = Objects.toString(parts.next());

    final StringBuilder builder = new StringBuilder(next);
    while (parts.hasNext()) {
      next = Objects.toString(parts.next());

      builder
          .append(parts.hasNext() ? separator : lastSeparator)
          .append(next);
    }

    return builder.toString();
  }
}
