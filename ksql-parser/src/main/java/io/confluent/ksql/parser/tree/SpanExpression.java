/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import org.apache.kafka.streams.kstream.JoinWindows;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SpanExpression extends Node {

  private final long before;
  private final long after;
  private final TimeUnit timeUnit;

  public SpanExpression(final long before, final long after, final TimeUnit timeUnit) {
    this(Optional.empty(), before, after, timeUnit);
  }


  private SpanExpression(final Optional<NodeLocation> location, final long before, final long after,
                         final TimeUnit timeUnit) {
    super(location);
    this.before = before;
    this.after = after;
    this.timeUnit = timeUnit;
  }

  public JoinWindows joinWindow() {
    final JoinWindows joinWindow = JoinWindows.of(timeUnit.toMillis(before));
    return joinWindow.after(timeUnit.toMillis(after));
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(" SPAN ");
    if (before == after) {
      builder.append(before).append(' ').append(timeUnit);
    } else {
      builder.append('(').append(before).append(", ").append(after).append(") ").append(timeUnit);
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(before, after, timeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpanExpression spanExpression = (SpanExpression) o;
    return spanExpression.before == before && spanExpression.after == after
           && spanExpression.timeUnit == timeUnit;
  }

  // Visible for testing
  public long getBefore() {
    return before;
  }

  // Visible for testing
  public long getAfter() {
    return after;
  }

  // Visible for testing
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }


}
