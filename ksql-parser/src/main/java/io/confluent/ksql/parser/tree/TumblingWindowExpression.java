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

public class TumblingWindowExpression extends KsqlWindowExpression {

  private final long size;
  private final WindowExpression.WindowUnit sizeUnit;

  public TumblingWindowExpression(long size, WindowExpression.WindowUnit sizeUnit) {
    this(Optional.empty(), "", size, sizeUnit);
  }

  public TumblingWindowExpression(NodeLocation location, String windowName,
                                  long size, WindowExpression.WindowUnit sizeUnit) {
    this(Optional.of(location), windowName, size, sizeUnit);
  }

  private TumblingWindowExpression(Optional<NodeLocation> location, String windowName, long size,
                                  WindowExpression.WindowUnit sizeUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = sizeUnit;
  }

  public long getSize() {
    return size;
  }

  public WindowExpression.WindowUnit getSizeUnit() {
    return sizeUnit;
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
