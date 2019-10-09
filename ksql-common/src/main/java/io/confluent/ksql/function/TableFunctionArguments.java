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

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;

public class TableFunctionArguments {

  private final int udtfIndex;
  private final List<String> args;

  public TableFunctionArguments(final int index,  final List<String> args) {
    this.udtfIndex = index;
    this.args = ImmutableList.copyOf(Objects.requireNonNull(args, "args"));

    if (index < 0) {
      throw new IllegalArgumentException("index is negative: " + index);
    }
  }

  public int udtfIndex() {
    return udtfIndex;
  }

  public String arg(final int i) {
    return args.get(i);
  }

  public void ensureArgCount(final int expectedCount, final String functionName) {
    if (args.size() != expectedCount) {
      throw new KsqlException(
          String.format("Invalid parameter count for %s. Need %d args, got %d arg(s)",
              functionName, expectedCount, args.size()));
    }
  }

}
