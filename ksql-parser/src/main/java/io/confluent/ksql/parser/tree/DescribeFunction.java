/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

public class DescribeFunction extends Statement {
  private final String functionName;

  public DescribeFunction(final NodeLocation location, final String functionName) {
    super(Optional.ofNullable(location));
    this.functionName = Objects.requireNonNull(functionName, "can't be null");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DescribeFunction that = (DescribeFunction) o;
    return Objects.equals(functionName, that.functionName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionName);
  }

  @Override
  public String toString() {
    return "DescribeFunction{"
        + "function='"
        + functionName + '\''
        + '}';
  }

  public String getFunctionName() {
    return functionName;
  }
}
