/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import io.confluent.ksql.schema.ksql.LogicalSchema;

final class QueryLogicalPlanUtil {

  private QueryLogicalPlanUtil() {
  }

  /**
   * Builds the schema used for codegen to compile expressions into bytecode. The input schema may
   * need to be extended with system columns if they are part of the projection.
   * @return the intermediate schema
   */
  static LogicalSchema buildIntermediateSchema(
      final LogicalSchema schema,
      final boolean addAdditionalColumnsToIntermediateSchema,
      final boolean isWindowed
  ) {
    if (!addAdditionalColumnsToIntermediateSchema) {
      return schema;
    } else {
      return schema.withPseudoAndKeyColsInValue(isWindowed, true);
    }
  }
}
