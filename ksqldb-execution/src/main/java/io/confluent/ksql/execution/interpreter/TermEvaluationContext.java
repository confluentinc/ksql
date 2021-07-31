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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.GenericRow;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;

/**
 * Holds all state associated with row-based evaluation of a {@code Term} tree. This for example
 * consists of the current row itself and any lambda-created variables that are in the current
 * call frame.
 */
public final class TermEvaluationContext {

  private final GenericRow row;
  private final Deque<Map<String, Object>> variableMappingsStack = new LinkedList<>();

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public TermEvaluationContext(final GenericRow row) {
    this.row = row;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public GenericRow getRow() {
    return row;
  }

  /**
   * Used to push a set of variables and their object mappings into the evaluation context.  This is
   * used for lambdas where such variables are created.
   * @param mappings The new mappings
   */
  public void pushVariableMappings(final Map<String, Object> mappings) {
    variableMappingsStack.push(mappings);
  }

  /**
   * Used to pop the set of variables that were mapped by a previous call to
   * {@code pushVariableMappings}. The calls to each of these methods is meant to be invoked
   * alongside the start and completion of the calling stack of the lambda.
   */
  public void popVariableMappings() {
    variableMappingsStack.pop();
  }

  /**
   * Looks up a variable from the mappings made with {@code pushVariableMappings} that have not yet
   * been popped with {@code popVariableMappings}.
   * @param name The name of the variable to lookup
   * @return the variable
   */
  public Object lookupVariable(final String name) {
    for (Map<String, Object> mappings : variableMappingsStack) {
      if (mappings.containsKey(name)) {
        return mappings.get(name);
      }
    }
    throw new IllegalStateException("Can't find lambda variable " + name);
  }
}
