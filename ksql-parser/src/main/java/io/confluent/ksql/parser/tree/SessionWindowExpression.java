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

public class SessionWindowExpression extends KsqlWindowExpression {

  private final long gap;
  private final TimeUnit sizeUnit;

  public SessionWindowExpression(long gap, TimeUnit sizeUnit) {
    this(Optional.empty(), "", gap, sizeUnit);
  }

  public SessionWindowExpression(NodeLocation location, String windowName,
                                 long gap, TimeUnit sizeUnit) {
    this(Optional.of(location), windowName, gap, sizeUnit);
  }

  private SessionWindowExpression(Optional<NodeLocation> location, String windowName, long gap,
                                  TimeUnit sizeUnit) {
    super(location);
    this.gap = gap;
    this.sizeUnit = sizeUnit;
  }

  public long getGap() {
    return gap;
  }

  public TimeUnit getSizeUnit() {
    return sizeUnit;
  }

  @Override
  public String toString() {
    return " SESSION ( " + gap + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(gap, sizeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SessionWindowExpression sessionWindowExpression = (SessionWindowExpression) o;
    return sessionWindowExpression.gap == gap && sessionWindowExpression.sizeUnit == sizeUnit;
  }
}
