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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.kstream.JoinWindows;

public class WithinExpression extends Node {

  private final long before;
  private final long after;
  private final TimeUnit beforeTimeUnit;
  private final TimeUnit afterTimeUnit;
  private final JoinWindows joinWindows;

  public WithinExpression(final long size, final TimeUnit timeUnit) {
    this(size, size, timeUnit, timeUnit);
  }

  public WithinExpression(final long before, final long after, final TimeUnit beforeTimeUnit,
                          final TimeUnit afterTimeUnit) {
    this(Optional.empty(), before, after, beforeTimeUnit, afterTimeUnit);
  }


  private WithinExpression(final Optional<NodeLocation> location, final long before,
                           final long after, final TimeUnit beforeTimeUnit,
                           final TimeUnit afterTimeUnit) {
    super(location);
    this.before = before;
    this.after = after;
    this.beforeTimeUnit = beforeTimeUnit;
    this.afterTimeUnit = afterTimeUnit;
    this.joinWindows = createJoinWindows();
  }

  public JoinWindows joinWindow() {
    return joinWindows;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append(" WITHIN ");
    if (before == after) {
      builder.append(before).append(' ').append(beforeTimeUnit);
    } else {
      builder.append('(')
          .append(before)
          .append(' ')
          .append(beforeTimeUnit)
          .append(", ")
          .append(after)
          .append(' ')
          .append(afterTimeUnit)
          .append(")");
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(before, after, beforeTimeUnit, afterTimeUnit);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WithinExpression withinExpression = (WithinExpression) o;
    return before == withinExpression.before && after == withinExpression.after
           && Objects.equals(beforeTimeUnit, withinExpression.beforeTimeUnit)
           && Objects.equals(afterTimeUnit, withinExpression.afterTimeUnit);
  }

  private JoinWindows createJoinWindows() {
    final JoinWindows joinWindow = JoinWindows.of(beforeTimeUnit.toMillis(before));
    return joinWindow.after(afterTimeUnit.toMillis(after));
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
  public TimeUnit getBeforeTimeUnit() {
    return beforeTimeUnit;
  }

  // Visible For Testing
  public TimeUnit getAfterTimeUnit() {
    return afterTimeUnit;
  }


}
