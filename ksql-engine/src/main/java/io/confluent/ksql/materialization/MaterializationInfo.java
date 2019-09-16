/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Optional;

/**
 * Pojo for passing around information about materialization of a query's state store
 */
@Immutable
public final class MaterializationInfo {

  private final String stateStoreName;
  private final LogicalSchema aggregationSchema;
  private final Optional<Expression> havingExpression;
  private final LogicalSchema tableSchema;
  private final List<SelectExpression> tableSelects;

  /**
   * Create instance.
   *
   * @param stateStoreName the name of the state store
   * @param stateStoreSchema the schema of the state store
   * @param havingExpression optional HAVING expression that should be apply to any store result.
   * @param tableSchema the schema of the table.
   * @param tableSelects SELECT expressions to convert state store schema to table schema.
   * @return instance.
   */
  public static MaterializationInfo of(
      final String stateStoreName,
      final LogicalSchema stateStoreSchema,
      final Optional<Expression> havingExpression,
      final LogicalSchema tableSchema,
      final List<SelectExpression> tableSelects
  ) {
    return new MaterializationInfo(
        stateStoreName,
        stateStoreSchema,
        havingExpression,
        tableSchema,
        tableSelects
    );
  }

  public String stateStoreName() {
    return stateStoreName;
  }

  public LogicalSchema aggregationSchema() {
    return aggregationSchema;
  }

  public Optional<Expression> havingExpression() {
    return havingExpression;
  }

  public LogicalSchema tableSchema() {
    return tableSchema;
  }

  public List<SelectExpression> tableSelects() {
    return tableSelects;
  }

  private MaterializationInfo(
      final String stateStoreName,
      final LogicalSchema aggregationSchema,
      final Optional<Expression> havingExpression,
      final LogicalSchema tableSchema,
      final List<SelectExpression> tableSelects
  ) {
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.aggregationSchema = requireNonNull(aggregationSchema, "aggregationSchema");
    this.havingExpression = requireNonNull(havingExpression, "havingExpression");
    this.tableSchema = requireNonNull(tableSchema, "tableSchema");
    this.tableSelects = ImmutableList.copyOf(requireNonNull(tableSelects, "tableSelects"));
  }
}

