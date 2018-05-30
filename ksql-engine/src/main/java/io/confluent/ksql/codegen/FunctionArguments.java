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

package io.confluent.ksql.codegen;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class FunctionArguments {
  private Stack<List<Schema.Type>> args = new Stack<>();

  public void beginFunction() {
    args.push(new ArrayList<>());
  }

  public void addArgumentType(final Schema.Type type) {
    if (!args.isEmpty()) {
      args.peek().add(type);
    }
  }

  public List<Schema.Type> endFunction() {
    return args.pop();
  }

  public void mergeArguments(final int from) {
    final List<Schema.Type> functionArgs = args.peek();
    final Schema.Type first = functionArgs.remove(from);
    final Schema.Type second = functionArgs.remove(from);
    functionArgs.add(first.ordinal() < second.ordinal() ? second : first);
  }

  public int numCurrentFunctionArguments() {
    return hasArgs() ? args.peek().size() : 0;
  }

  private boolean hasArgs() {
    return !args.isEmpty();
  }
}
