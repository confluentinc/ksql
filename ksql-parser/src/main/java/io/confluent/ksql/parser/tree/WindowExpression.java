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

public class WindowExpression extends Node {

  private final String windowName;
  private  final KsqlWindowExpression ksqlWindowExpression;

  public WindowExpression(String windowName, KsqlWindowExpression ksqlWindowExpression) {
    this(Optional.empty(), windowName, ksqlWindowExpression);
  }

  protected WindowExpression(Optional<NodeLocation> location, String windowName,
                             KsqlWindowExpression ksqlWindowExpression) {
    super(location);
    this.windowName = windowName;
    this.ksqlWindowExpression = ksqlWindowExpression;
  }

  public KsqlWindowExpression getKsqlWindowExpression() {
    return ksqlWindowExpression;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WindowExpression o = (WindowExpression) obj;
    return Objects.equals(ksqlWindowExpression, o.ksqlWindowExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowName, ksqlWindowExpression);
  }

  @Override
  public String toString() {
    return " WINDOW " + windowName + " " + ksqlWindowExpression.toString();
  }


  public static TimeUnit getWindowUnit(String windowUnitString) {
    try {
      if (!windowUnitString.endsWith("S")) {
        return TimeUnit.valueOf(windowUnitString + "S");
      }
      return TimeUnit.valueOf(windowUnitString);
    } catch (IllegalArgumentException | NullPointerException e) {
      return null;
    }
  }

}
