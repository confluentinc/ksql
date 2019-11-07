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
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Immutable
public class HoppingWindowExpression extends KsqlWindowExpression {

  private final long size;
  private final TimeUnit sizeUnit;
  private final long advanceBy;
  private final TimeUnit advanceByUnit;

  public HoppingWindowExpression(
      long size, TimeUnit sizeUnit, long advanceBy, TimeUnit advanceByUnit
  ) {
    this(Optional.empty(), size, sizeUnit, advanceBy, advanceByUnit);
  }

  public HoppingWindowExpression(
      Optional<NodeLocation> location, long size, TimeUnit sizeUnit, long advanceBy,
      TimeUnit advanceByUnit
  ) {
    super(location);
    this.size = size;
    this.sizeUnit = requireNonNull(sizeUnit, "sizeUnit");
    this.advanceBy = advanceBy;
    this.advanceByUnit = requireNonNull(advanceByUnit, "advanceByUnit");
  }

  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(
        WindowType.HOPPING,
        Optional.of(Duration.ofNanos(sizeUnit.toNanos(size)))
    );
  }

  public TimeUnit getSizeUnit() {
    return sizeUnit;
  }

  public long getSize() {
    return size;
  }

  public TimeUnit getAdvanceByUnit() {
    return advanceByUnit;
  }

  public long getAdvanceBy() {
    return advanceBy;
  }

  @Override
  public <R, C> R accept(WindowVisitor<R, C> visitor, C context) {
    return visitor.visitHoppingWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " HOPPING ( SIZE " + size + " " + sizeUnit + " , ADVANCE BY "
        + advanceBy + " " + "" + advanceByUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, sizeUnit, advanceBy, advanceByUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression) o;
    return hoppingWindowExpression.size == size && hoppingWindowExpression.sizeUnit == sizeUnit
        && hoppingWindowExpression.advanceBy == advanceBy && hoppingWindowExpression
        .advanceByUnit == advanceByUnit;
  }
}
