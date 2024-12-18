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

@Immutable
public final class NodeLocation {

  private final int line;
  private final int charPositionInLine;

  public NodeLocation(final int line, final int charPositionInLine) {
    this.line = line;
    this.charPositionInLine = charPositionInLine;
  }

  /**
   * @return the line number within the statement where the {@link Node} begins.
   *     Note: the line numbers start from 1 (and not 0).
   */
  public int getStartLineNumber() {
    return getLineNumber();
  }

  /**
   * @return the column number within the statement where the {@link Node} begins.
   *     Note: the column numbers start from 1 (and not 0)
   */
  public int getStartColumnNumber() {
    return getColumnNumber();
  }

  public int getLineNumber() {
    return line;
  }

  public int getColumnNumber() {
    return charPositionInLine + 1;
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
    return String.format("Line: %d, Col: %d", line, charPositionInLine + 1);
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
    return line == that.line
        && charPositionInLine == that.charPositionInLine;
  }

  @Override
  public int hashCode() {
    return Objects.hash(line, charPositionInLine);
  }
}
