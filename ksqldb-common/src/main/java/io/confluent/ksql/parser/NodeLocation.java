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

  private final TokenLocation start;
  private final TokenLocation stop;

  public NodeLocation(final TokenLocation start, final TokenLocation stop) {
    this.start = start;
    this.stop = stop;
  }

  public NodeLocation(final int startLine, final int startCharPositionInLine) {
    this.start = TokenLocation.of(startLine, startCharPositionInLine);
    this.stop = TokenLocation.empty();
  }

  public int getStartLineNumber() {
    return start.getLine().getAsInt();
  }

  public int getStartColumnNumber() {
    return start.getCharPositionInLine().getAsInt() + 1;
  }

  public OptionalInt getStopLine() {
    return stop.getLine();
  }

  public OptionalInt getStopColumnNumber() {
    return OptionalInt.of(stop.getCharPositionInLine().getAsInt()
        + stop.getStopIndex().getAsInt()
        - stop.getStartIndex().getAsInt()
        + 1);
  }

  public OptionalInt getLength() {
    return OptionalInt.of(stop.getStopIndex().getAsInt() - start.getStartIndex().getAsInt() + 1);
  }

  public TokenLocation getStartTokenLocation() {
    return start;
  }

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
