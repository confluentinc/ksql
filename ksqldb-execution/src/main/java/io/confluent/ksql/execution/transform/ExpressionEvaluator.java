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

package io.confluent.ksql.execution.transform;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.function.Supplier;

/**
 * This is an interface for something that is able to evaluate an expression on a given row.
 * Different version of this may compile the expression or interpret it.
 */
public interface ExpressionEvaluator {
  Object evaluate(
      GenericRow row,
      Object defaultValue,
      ProcessingLogger logger,
      Supplier<String> errorMsg
  );

  Expression getExpression();

  SqlType getExpressionType();
}
