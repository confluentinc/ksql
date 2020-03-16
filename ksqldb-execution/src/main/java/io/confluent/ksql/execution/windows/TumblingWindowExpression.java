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

package io.confluent.ksql.execution.windows;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class TumblingWindowExpression extends KsqlWindowExpression {

  private final WindowTimeClause size;

  public TumblingWindowExpression(final WindowTimeClause size) {
    this(Optional.empty(), size, Optional.empty(), Optional.empty());
  }

  public TumblingWindowExpression(
      final Optional<NodeLocation> location,
      final WindowTimeClause size,
      final Optional<WindowTimeClause> retention,
      final Optional<WindowTimeClause> gracePeriod
  ) {
    super(location, retention, gracePeriod);
    this.size = requireNonNull(size, "size");
  }

  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(
        WindowType.TUMBLING,
        Optional.of(size.toDuration())
    );
  }

  public WindowTimeClause getSize() {
    return size;
  }

  @Override
  public <R, C> R accept(final WindowVisitor<R, C> visitor, final C context) {
    return visitor.visitTumblingWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " TUMBLING ( SIZE " + size
        + retention.map(w -> " , RETENTION " + w).orElse("")
        + gracePeriod.map(g -> " , GRACE PERIOD " + g).orElse("")
        + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, retention, gracePeriod);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression) o;
    return Objects.equals(size, tumblingWindowExpression.size)
        && Objects.equals(gracePeriod, tumblingWindowExpression.gracePeriod)
        && Objects.equals(retention, tumblingWindowExpression.retention);
  }
}
