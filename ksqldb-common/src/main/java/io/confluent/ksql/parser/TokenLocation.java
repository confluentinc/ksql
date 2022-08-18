/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.parser;

import com.google.errorprone.annotations.Immutable;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Wrapper class for encapsulating the location information of the
 * antlr Token {@link org.antlr.v4.runtime.Token}
 */
@Immutable
public final class TokenLocation {

  /**
   * line number returned by {@link org.antlr.v4.runtime.Token#getLine()}
   */
  private final OptionalInt line;

  /**
   * position of character in line returned
   * by {@link org.antlr.v4.runtime.Token#getCharPositionInLine()}
   */
  private final OptionalInt charPositionInLine;

  /**
   * start index returned by {@link org.antlr.v4.runtime.Token#getStartIndex()}
   */
  private final OptionalInt startIndex;

  /**
   * stop index returned by {@link org.antlr.v4.runtime.Token#getStopIndex()}
   */
  private final OptionalInt stopIndex;

  public static TokenLocation empty() {
    return new TokenLocation(OptionalInt.empty(),
        OptionalInt.empty(),
        OptionalInt.empty(),
        OptionalInt.empty());
  }

  public static TokenLocation of(final int line, final int charPositionInLine) {
    return new TokenLocation(
        OptionalInt.of(line),
        OptionalInt.of(charPositionInLine),
        OptionalInt.empty(),
        OptionalInt.empty()
    );
  }

  /**
   * @param line line number returned by {@link org.antlr.v4.runtime.Token#getLine()}
   * @param charPositionInLine position of character in line returned
   *                           by {@link org.antlr.v4.runtime.Token#getCharPositionInLine()}
   * @param startIndex start index returned by {@link org.antlr.v4.runtime.Token#getStartIndex()}
   * @param stopIndex stop index returned by {@link org.antlr.v4.runtime.Token#getStopIndex()}
   */
  public TokenLocation(final OptionalInt line,
                       final OptionalInt charPositionInLine,
                       final OptionalInt startIndex,
                       final OptionalInt stopIndex) {
    this.line = line;
    this.charPositionInLine = charPositionInLine;
    this.startIndex = startIndex;
    this.stopIndex = stopIndex;
  }

  public OptionalInt getLine() {
    return line;
  }

  public OptionalInt getCharPositionInLine() {
    return charPositionInLine;
  }

  public OptionalInt getStartIndex() {
    return startIndex;
  }

  public OptionalInt getStopIndex() {
    return stopIndex;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TokenLocation that = (TokenLocation) o;
    return line.equals(that.line)
        && charPositionInLine.equals(that.charPositionInLine)
        && startIndex.equals(that.startIndex)
        && stopIndex.equals(that.stopIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(line, charPositionInLine, startIndex, stopIndex);
  }
}
