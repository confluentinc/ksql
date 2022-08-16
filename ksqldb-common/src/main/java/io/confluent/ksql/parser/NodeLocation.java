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

@Immutable
@SuppressWarnings("checkstyle:CyclomaticComplexity")
public final class NodeLocation {

  private final int startLine;
  private final int startCharPositionInLine;
  private final OptionalInt startStartIndex;
  private final OptionalInt startStopIndex;
  private final OptionalInt endLine;
  private final OptionalInt stopCharPositionInLine;
  private final OptionalInt stopStartIndex;
  private final OptionalInt stopStopIndex;

  public NodeLocation(final int startLine,
                      final int startCharPositionInLine,
                      final OptionalInt startStartIndex,
                      final OptionalInt startStopIndex,
                      final OptionalInt endLine,
                      final OptionalInt stopCharPositionInLine,
                      final OptionalInt stopStartIndex,
                      final OptionalInt stopStopIndex) {
    this.startLine = startLine;
    this.startCharPositionInLine = startCharPositionInLine;
    this.startStartIndex = startStartIndex;
    this.startStopIndex = startStopIndex;
    this.endLine = endLine;
    this.stopCharPositionInLine = stopCharPositionInLine;
    this.stopStartIndex = stopStartIndex;
    this.stopStopIndex = stopStopIndex;
  }

  public NodeLocation(final int startLine, final int startCharPositionInLine) {
    this.startLine = startLine;
    this.startCharPositionInLine = startCharPositionInLine;
    this.startStartIndex = OptionalInt.empty();
    this.startStopIndex = OptionalInt.empty();
    this.endLine = OptionalInt.empty();
    this.stopCharPositionInLine = OptionalInt.empty();
    this.stopStartIndex = OptionalInt.empty();
    this.stopStopIndex = OptionalInt.empty();
  }

  public int getLineNumber() {
    return startLine;
  }

  public int getColumnNumber() {
    return startCharPositionInLine + 1;
  }

  public OptionalInt getEndLine() {
    return endLine;
  }

  public OptionalInt getEndColumnNumber() {
    return OptionalInt.of(stopCharPositionInLine.getAsInt()
        + stopStopIndex.getAsInt()
        - stopStartIndex.getAsInt()
        + 1);
  }

  public OptionalInt getLength() {
    return OptionalInt.of(stopStopIndex.getAsInt() - startStartIndex.getAsInt() + 1);
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
    return String.format("Line: %d, Col: %d", startLine, startCharPositionInLine + 1);
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
    return startLine == that.startLine
        && startCharPositionInLine == that.startCharPositionInLine
        && startStartIndex == that.startStartIndex
        && startStopIndex == that.startStopIndex
        && endLine == that.endLine
        && stopCharPositionInLine == that.stopCharPositionInLine
        && stopStartIndex == that.stopStartIndex
        && stopStopIndex == that.stopStartIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startLine,
        startCharPositionInLine,
        startStartIndex,
        startStopIndex,
        endLine,
        stopCharPositionInLine,
        stopStartIndex,
        stopStopIndex);
  }
}
