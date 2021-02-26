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

package io.confluent.ksql.execution.codegen;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;

public final class TypeContextUtil {
  private TypeContextUtil() {

  }

  /**
   * Returns a copy of the appropriate context to use when processing an expression subtree.
   * A copy is required to prevent different subtrees from getting a context that's been
   * modified by another subtree. For non-lambdas we want to use the parent context because 
   * there may be valid overlapping lambda parameter names in different child nodes. For
   * lambdas, we want to use the updateContext which has the type information the lambda
   * expression body needs.
   *
   * @param expression the current expression we're processing
   * @param parentContext the context passed into the parent node of the expression
   * @param updatedContext the context that has been updated as we processed other child
   *                       nodes
   * @return a copy of either parent or current type context to be passed to the child
   */
  public static TypeContext contextForExpression(
      final Expression expression,
      final TypeContext parentContext,
      final TypeContext updatedContext
  ) {
    if (expression instanceof LambdaFunctionCall) {
      return updatedContext.getCopy();
    } else {
      return parentContext.getCopy();
    }
  }
}
