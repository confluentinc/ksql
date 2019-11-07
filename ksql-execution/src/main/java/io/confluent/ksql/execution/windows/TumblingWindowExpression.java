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
public class TumblingWindowExpression extends KsqlWindowExpression {

  private final long size;
  private final TimeUnit sizeUnit;

  public TumblingWindowExpression(long size, TimeUnit sizeUnit) {
    this(Optional.empty(), size, sizeUnit);
  }

  public TumblingWindowExpression(Optional<NodeLocation> location, long size, TimeUnit sizeUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = requireNonNull(sizeUnit, "sizeUnit");
  }

  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(
        WindowType.TUMBLING,
        Optional.of(Duration.ofNanos(sizeUnit.toNanos(size)))
    );
  }

  public TimeUnit getSizeUnit() {
    return sizeUnit;
  }

  public long getSize() {
    return size;
  }

  @Override
  public <R, C> R accept(WindowVisitor<R, C> visitor, C context) {
    return visitor.visitTumblingWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " TUMBLING ( SIZE " + size + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, sizeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression) o;
    return tumblingWindowExpression.size == size && tumblingWindowExpression.sizeUnit == sizeUnit;
  }
}
