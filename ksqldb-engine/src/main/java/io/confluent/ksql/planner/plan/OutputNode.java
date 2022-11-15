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

package io.confluent.ksql.planner.plan;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;
import java.util.OptionalInt;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public abstract class OutputNode extends SingleSourcePlanNode {

  private final OptionalInt limit;
  private final Optional<TimestampColumn> timestampColumn;
  private final LogicalSchema schema;

  protected OutputNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final OptionalInt limit,
      final Optional<TimestampColumn> timestampColumn
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName(), source);

    this.schema = requireNonNull(schema, "schema");
    this.limit = requireNonNull(limit, "limit");
    this.timestampColumn =
        requireNonNull(timestampColumn, "timestampColumn");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  public OptionalInt getLimit() {
    return limit;
  }

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  public abstract Optional<SourceName> getSinkName();
}
