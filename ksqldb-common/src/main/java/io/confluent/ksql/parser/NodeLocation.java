/*
 * Copyright 2018 Confluent Inc.
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
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Entity that contains the location information of the start token
 * and the stop token. The terms "Token" and "token" refer to
 * {@link org.antlr.v4.runtime.Token}
 */
@Immutable
public final class NodeLocation {

  /**
   * location information for the start {@link org.antlr.v4.runtime.Token}
   * of the Node.
   */
  private final TokenLocation start;

  /**
   * location information for the stop {@link org.antlr.v4.runtime.Token}
   * of the Node.
   */
  private final TokenLocation stop;

  /**
   * @param start {@link TokenLocation} of the start {@link org.antlr.v4.runtime.Token}
   * @param stop {@link TokenLocation} of the stop {@link org.antlr.v4.runtime.Token}
   */
  public NodeLocation(final TokenLocation start, final TokenLocation stop) {
    this.start = start;
    this.stop = stop;
  }

  /**
   * @param startLine line number of the start token
   * @param startCharPositionInLine position of the first character of the start token
   *                                within the start line
   */
  public NodeLocation(final int startLine, final int startCharPositionInLine) {
    this.start = TokenLocation.of(startLine, startCharPositionInLine);
    this.stop = TokenLocation.empty();
  }

  /**
   * @return the line number within the statement where the {@link Node} begins.
   *     Note: the line numbers start from 1 (and not 0).
   */
  public int getStartLineNumber() {
    return start.getLine().getAsInt();
  }

  /**
   * @return the column number within the statement where the {@link Node} begins.
   *     Note: the column numbers start from 1 (and not 0)
   */
  public int getStartColumnNumber() {
    return start.getCharPositionInLine().getAsInt() + 1;
  }

  /**
   * @return the line number within the statement where the {@link Node} ends.
   *     Note: the line numbers start from 1 (and not 0)
   */
  public OptionalInt getStopLine() {
    return stop.getLine();
  }

  /**
   * @return the column number within the statement where the {@link Node} end.
   *     Note: the column numbers start from 1 (and not 0)
   */
  public OptionalInt getStopColumnNumber() {
    return OptionalInt.of(stop.getCharPositionInLine().getAsInt()
        + stop.getStopIndex().getAsInt()
        - stop.getStartIndex().getAsInt()
        + 1);
  }

  /**
   * @return the length of the statement represented by the {@link Node}
   */
  public OptionalInt getLength() {
    return OptionalInt.of(stop.getStopIndex().getAsInt() - start.getStartIndex().getAsInt() + 1);
  }

  /**
   * @return the {@link TokenLocation} of the start {@link org.antlr.v4.runtime.Token}
   */
  public TokenLocation getStartTokenLocation() {
    return start;
  }

  /**
   * @return the {@link TokenLocation} of the stop {@link org.antlr.v4.runtime.Token}
   */
  public TokenLocation getStopTokenLocation() {
    return stop;
  }

  public String asPrefix() {
    return toString() + ": ";
  }

  public static String asPrefix(final Optional<NodeLocation> location) {
    return location
        .map(NodeLocation::asPrefix)
        .orElse("");
  }

  @Override
  public String toString() {
    return String.format("Line: %d, Col: %d",
        start.getLine().getAsInt(),
        start.getCharPositionInLine().getAsInt() + 1);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final NodeLocation that = (NodeLocation) o;
    return start.equals(that.start)
        && stop.equals(that.stop);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, stop);
  }
}
