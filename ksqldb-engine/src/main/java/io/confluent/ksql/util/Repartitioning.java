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

package io.confluent.ksql.util;

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;

/**
 * Util class around repartitioning.
 */
public final class Repartitioning {

  private Repartitioning() {
  }

  /**
   * Determine if a repartition is needed.
   *
   * <p>A repartition is only not required if partitioning by the existing key columns.
   *
   * @param schema the schema of the data before any repartition
   * @param partitionBy the expressions to partition by
   * @return {@code true} if a repartition is needed.
   */
  public static boolean repartitionNeeded(
      final LogicalSchema schema,
      final List<Expression> partitionBy
  ) {
    if (schema.key().isEmpty()) {
      // No current key, so repartition needed:
      return true;
    }

    // this is technically covered by the check below because our syntax does not
    // yet support PARTITION BY col1, col2 but we make this explicit for when we
    // do end up supporting this (we'll have to change the logic here)
    if (schema.key().size() != 1) {
      return true;
    }

    if (partitionBy.size() != schema.key().size()) {
      // Different number of expressions to keys means it must be a repartition:
      return true;
    }

    final Expression expression = partitionBy.get(0);
    if (!(expression instanceof ColumnReferenceExp)) {
      // If expression is not a column reference then the key will be changing
      return true;
    }

    final ColumnName newKeyColName = ((ColumnReferenceExp) expression).getColumnName();

    return !newKeyColName.equals(schema.key().get(0).name());
  }
}
