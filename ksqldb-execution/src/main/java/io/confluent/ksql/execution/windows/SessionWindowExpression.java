/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.windows;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class SessionWindowExpression extends KsqlWindowExpression {

  private final WindowTimeClause gap;

  public SessionWindowExpression(final WindowTimeClause gap) {
    this(Optional.empty(), gap, Optional.empty(), Optional.empty());
  }

  public SessionWindowExpression(
      final Optional<NodeLocation> location,
      final WindowTimeClause gap,
      final Optional<WindowTimeClause> retention,
      final Optional<WindowTimeClause> gracePeriod
  ) {
    super(location, retention, gracePeriod);
    this.gap = requireNonNull(gap, "gap");
  }

  public WindowTimeClause getGap() {
    return gap;
  }

  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(WindowType.SESSION, Optional.empty());
  }

  @Override
  public <R, C> R accept(final WindowVisitor<R, C> visitor, final C context) {
    return visitor.visitSessionWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " SESSION ( " + gap
        + retention.map(w -> " , RETENTION " + w).orElse("")
        + gracePeriod.map(g -> " , GRACE PERIOD " + g).orElse("")
        + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(gap, retention, gracePeriod);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SessionWindowExpression sessionWindowExpression = (SessionWindowExpression) o;
    return Objects.equals(gap, sessionWindowExpression.gap)
        && Objects.equals(gracePeriod, sessionWindowExpression.gracePeriod)
        && Objects.equals(retention, sessionWindowExpression.retention);
  }
}
