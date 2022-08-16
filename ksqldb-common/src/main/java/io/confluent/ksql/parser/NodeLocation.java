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
public final class NodeLocation {

  private final int startLine;
  private final int startStartIndex;
  private final OptionalInt startStopIndex;
  private final OptionalInt endLine;
  private final OptionalInt stopStartIndex;
  private final OptionalInt stopStopIndex;

  public NodeLocation(final int startLine,
                      final int startStartIndex,
                      final OptionalInt startStopIndex,
                      final OptionalInt endLine,
                      final OptionalInt stopStartIndex,
                      final OptionalInt stopStopIndex) {
    this.startLine = startLine;
    this.startStartIndex = startStartIndex;
    this.startStopIndex = startStopIndex;
    this.endLine = endLine;
    this.stopStartIndex = stopStartIndex;
    this.stopStopIndex = stopStopIndex;
  }

  public NodeLocation(final int startLine, final int startStartIndex) {
    this.startLine = startLine;
    this.startStartIndex = startStartIndex;
    this.startStopIndex = OptionalInt.empty();
    this.endLine = OptionalInt.empty();
    this.stopStartIndex = OptionalInt.empty();
    this.stopStopIndex = OptionalInt.empty();
  }

  public int getLineNumber() {
    return startLine;
  }

  public int getColumnNumber() {
    return startStartIndex + 1;
  }

  public OptionalInt getEndLineNumber() {
    return endLine;
  }

  public OptionalInt getEndColumnNumber() {
    return this.stopStartIndex;
  }

  public OptionalInt getStopStopIndex() {
    return this.stopStopIndex;
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
    return String.format("Line: %d, Col: %d", startLine, startStartIndex + 1);
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
        && startStartIndex == that.startStartIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startLine, startStartIndex);
  }
}
