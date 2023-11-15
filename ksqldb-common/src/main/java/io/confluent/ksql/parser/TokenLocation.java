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

/**
 * Wrapper class for encapsulating the location information of the
 * antlr Token {@link org.antlr.v4.runtime.Token}
 */
@Immutable
public final class TokenLocation {

  /**
   * line number returned by {@link org.antlr.v4.runtime.Token#getLine()}
   */
  private final int line;

  /**
   * position of character in line returned
   * by {@link org.antlr.v4.runtime.Token#getCharPositionInLine()}
   */
  private final int charPositionInLine;

  /**
   * start index returned by {@link org.antlr.v4.runtime.Token#getStartIndex()}
   */
  private final int startIndex;

  /**
   * stop index returned by {@link org.antlr.v4.runtime.Token#getStopIndex()}
   */
  private final int stopIndex;

  public static TokenLocation of(final int line, final int charPositionInLine) {
    return new TokenLocation(
        line,
        charPositionInLine,
        Integer.MAX_VALUE,
        Integer.MAX_VALUE
    );
  }

  /**
   * @param line line number returned by {@link org.antlr.v4.runtime.Token#getLine()}
   * @param charPositionInLine position of character in line returned
   *                           by {@link org.antlr.v4.runtime.Token#getCharPositionInLine()}
   * @param startIndex start index returned by {@link org.antlr.v4.runtime.Token#getStartIndex()}
   * @param stopIndex stop index returned by {@link org.antlr.v4.runtime.Token#getStopIndex()}
   */
  public TokenLocation(final int line,
                       final int charPositionInLine,
                       final int startIndex,
                       final int stopIndex) {
    this.line = line;
    this.charPositionInLine = charPositionInLine;
    this.startIndex = startIndex;
    this.stopIndex = stopIndex;
  }

  public int getLine() {
    return line;
  }

  public int getCharPositionInLine() {
    return charPositionInLine;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getStopIndex() {
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
    return line == that.line
        && charPositionInLine == that.charPositionInLine
        && startIndex == that.startIndex
        && stopIndex == that.stopIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(line, charPositionInLine, startIndex, stopIndex);
  }
}
