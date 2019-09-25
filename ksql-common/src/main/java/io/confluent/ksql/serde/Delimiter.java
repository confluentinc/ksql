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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

@Immutable
public final class Delimiter {

  public final char delimiter;

  public static Delimiter parse(final char ch) {
    return new Delimiter(ch);
  }

  private Delimiter(final char delimiter) {
    this.delimiter = delimiter;
  }

  public static Delimiter parse(final String str) {
    if (str == null) {
      throw new NullPointerException();
    }
    if (str.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Delimiter cannot be empty, if you meant to have a tab or space for delimiter, please "
          + "use the special values 'TAB' or 'SPACE'"
          + System.lineSeparator()
          + "Example valid value: ';'"
      );
    }
    if (str.length() == 1) {
      return new Delimiter(str.charAt(0));
    }
    final Character delim = NAMED_DELIMITERS.get(str);
    if (delim != null) {
      return new Delimiter(delim);
    }
    throw new IllegalArgumentException(
        "Delimiter must be a single character or "
        + NAMED_DELIMITERS_STRING
        + System.lineSeparator()
        + "Example valid value: ';'"
    );
  }

  private static final Map<String, Character> NAMED_DELIMITERS = ImmutableMap
      .<String, Character>builder()
      .put("TAB", '\t')
      .put("SPACE", ' ')
      .build();

  private static final String NAMED_DELIMITERS_STRING = getNamedDelimitersString();

  private static String getNamedDelimitersString() {
    final StringBuilder sb = new StringBuilder();
    final Iterator<String> iter = NAMED_DELIMITERS.keySet().iterator();
    while (iter.hasNext()) {
      sb.append(iter.next());
      if (iter.hasNext()) {
        sb.append(", ");
      }
    }
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Delimiter delimiter1 = (Delimiter) o;
    return delimiter == delimiter1.delimiter;
  }

  @Override
  public int hashCode() {
    return Objects.hash(delimiter);
  }

  @Override
  public String toString() {
    return String.valueOf(delimiter);
  }
}
