/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.interpreter;

import io.confluent.ksql.GenericRow;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

public final class TermEvaluationContext {

  private final GenericRow row;
  private final Deque<Map<String, Object>> deque = new LinkedList<>();

  public TermEvaluationContext(final GenericRow row) {
    this.row = row;
  }

  public GenericRow getRow() {
    return row;
  }

  public void pushVariableMappings(final Map<String, Object> mappings) {
    deque.push(mappings);
  }

  public void popVariableMappings() {
    deque.pop();
  }

  public Object lookupVariable(final String name) {
    for (Map<String, Object> mappings : deque) {
      if (mappings.containsKey(name)) {
        return mappings.get(name);
      }
    }
    throw new IllegalStateException("Can't find lambda variable " + name);
  }
}
