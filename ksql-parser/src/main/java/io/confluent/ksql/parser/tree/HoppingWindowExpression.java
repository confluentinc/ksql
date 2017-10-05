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

public class HoppingWindowExpression extends KsqlWindowExpression {

  private final long size;
  private final WindowExpression.WindowUnit sizeUnit;
  private final long advanceBy;
  private final WindowExpression.WindowUnit advanceByUnit;

  public HoppingWindowExpression(long size, WindowExpression.WindowUnit sizeUnit,
                                  long advanceBy, WindowExpression.WindowUnit advanceByUnit) {
    this(Optional.empty(), "", size, sizeUnit, advanceBy, advanceByUnit);
  }

  public HoppingWindowExpression(NodeLocation location, String windowName, long size,
                                 WindowExpression.WindowUnit
      sizeUnit,
                                   long advanceBy, WindowExpression.WindowUnit advanceByUnit) {
    this(Optional.of(location), windowName, size, sizeUnit, advanceBy, advanceByUnit);
  }

  private HoppingWindowExpression(Optional<NodeLocation> location, String windowName, long size,
                                   WindowExpression.WindowUnit sizeUnit,
                             long advanceBy, WindowExpression.WindowUnit advanceByUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = sizeUnit;
    this.advanceBy = advanceBy;
    this.advanceByUnit = advanceByUnit;
  }

  public long getSize() {
    return size;
  }

  public WindowExpression.WindowUnit getSizeUnit() {
    return sizeUnit;
  }

  public long getAdvanceBy() {
    return advanceBy;
  }

  public WindowExpression.WindowUnit getAdvanceByUnit() {
    return advanceByUnit;
  }

  @Override
  public String toString() {
    return " HOPPING ( SIZE " + size + " " + sizeUnit + " , ADVANCE BY "
           + advanceBy + " "
           + "" + advanceByUnit + " ) ";
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
