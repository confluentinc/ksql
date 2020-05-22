/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.planner.plan;

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.SourceName;
import java.util.Set;

public interface VerifiableNode {

  /**
   * Throws if the key is not present in the projection.
   *
   * @param sinkName the name of the source being built.
   */
  void validateKeyPresent(SourceName sinkName);

  /**
   * Called to validate that columns referenced in the query are valid, i.e. they are known.
   *
   * @param functionRegistry the function registry.
   * @return any unknown columns.
   */
  Set<ColumnReferenceExp> validateColumns(FunctionRegistry functionRegistry);
}
