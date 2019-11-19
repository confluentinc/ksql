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

package io.confluent.ksql.execution.materialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.LinkedList;
import java.util.List;

/**
 * Pojo for passing around information about materialization of a query's state store
 */
@Immutable
public final class MaterializationInfo {

  private final String stateStoreName;
  private final LogicalSchema stateStoreSchema;
  private final List<TransformInfo> transforms;
  private final LogicalSchema schema;

  public String stateStoreName() {
    return stateStoreName;
  }

  public LogicalSchema getStateStoreSchema() {
    return stateStoreSchema;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public List<TransformInfo> getTransforms() {
    return transforms;
  }

  private MaterializationInfo(
      String stateStoreName, LogicalSchema stateStoreSchema, List<TransformInfo> transforms,
      LogicalSchema schema
  ) {
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.stateStoreSchema = requireNonNull(stateStoreSchema, "stateStoreSchema");
    this.transforms = ImmutableList.copyOf(requireNonNull(transforms, "transforms"));
    this.schema = requireNonNull(schema, "schema");
  }

  /**
   * Create a MaterializationInfo builder.
   *
   * @param stateStoreName   the name of the state store
   * @param stateStoreSchema the schema of the data in the state store
   * @return builder instance.
   */
  public static Builder builder(String stateStoreName, LogicalSchema stateStoreSchema) {
    return new Builder(stateStoreName, stateStoreSchema);
  }

  public static final class Builder {

    private final String stateStoreName;
    private final LogicalSchema stateStoreSchema;
    private final List<TransformInfo> transforms;
    private LogicalSchema schema;

    private Builder(String stateStoreName, LogicalSchema stateStoreSchema) {
      this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
      this.stateStoreSchema = dropMetaColumns(requireNonNull(stateStoreSchema, "stateStoreSchema"));
      this.transforms = new LinkedList<>();
      this.schema = dropMetaColumns(stateStoreSchema);
    }

    /**
     * Adds an aggregate map transform for mapping final result of complex aggregates (e.g. avg)
     *
     * @param aggregatesInfo descriptor of aggregation functions.
     * @param resultSchema   schema after applying aggregate result mapping.
     * @return A builder instance with this transformation.
     */
    public Builder mapAggregates(AggregatesInfo aggregatesInfo, LogicalSchema resultSchema) {
      if (resultSchema.value().size() != aggregatesInfo.valueColumnCount()) {
        throw new IllegalArgumentException("value column count mismatch. "
            + "Expected: " + aggregatesInfo.valueColumnCount() + ", "
            + "Got: " + resultSchema.value().size()
        );
      }

      transforms.add(new AggregateMapInfo(aggregatesInfo));
      this.schema = dropMetaColumns(resultSchema);
      return this;
    }

    /**
     * Adds a transform that projects a list of expressions from the value.
     *
     * @param selectExpressions The list of expressions to project.
     * @param resultSchema      The schema after applying the projection.
     * @return A builder instance with this transformation.
     */
    public Builder project(List<SelectExpression> selectExpressions, LogicalSchema resultSchema) {
      if (resultSchema.value().size() != selectExpressions.size()) {
        throw new IllegalArgumentException("value column count mismatch. "
            + "Expected: " + selectExpressions.size() + ", "
            + "Got: " + resultSchema.value().size()
        );
      }
      transforms.add(new ProjectInfo(selectExpressions, this.schema));
      this.schema = dropMetaColumns(resultSchema);
      return this;
    }

    /**
     * Adds a transform that filters rows from the materialization.
     *
     * @param filterExpression A boolean expression to filter rows on.
     * @return A builder instance with this transformation.
     */
    public Builder filter(Expression filterExpression) {
      transforms.add(new SqlPredicateInfo(filterExpression, schema));
      return this;
    }

    /**
     * Builds a MaterializationInfo with the properties and transforms in the builder.
     *
     * @return a MaterializationInfo instance.
     */
    public MaterializationInfo build() {
      return new MaterializationInfo(stateStoreName, stateStoreSchema, transforms, schema);
    }

    /*
     * Materialized tables do not have meta columns, such as ROWTIME, as this would be obtained
     * from the source event triggering the output in push / persistent query. Materialized tables
     * are accessed by pull queries, which have no source events triggering output.
     */
    private static LogicalSchema dropMetaColumns(final LogicalSchema schema) {
      return schema.withoutMetaColumns();
    }
  }

  public interface TransformInfo {

    <R> R visit(TransformVisitor<R> visitor);
  }

  public interface TransformVisitor<R> {

    R visit(AggregateMapInfo mapInfo);

    R visit(SqlPredicateInfo info);

    R visit(ProjectInfo info);
  }

  public static class AggregateMapInfo implements TransformInfo {

    final AggregatesInfo info;

    AggregateMapInfo(AggregatesInfo info) {
      this.info = requireNonNull(info, "info");
    }

    public AggregatesInfo getInfo() {
      return info;
    }

    public <R> R visit(TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class SqlPredicateInfo implements TransformInfo {

    final Expression filterExpression;
    final LogicalSchema schema;

    SqlPredicateInfo(Expression filterExpression, LogicalSchema schema) {
      this.filterExpression = requireNonNull(filterExpression, "filterExpression");
      this.schema = requireNonNull(schema, "schema");
    }

    public Expression getFilterExpression() {
      return filterExpression;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public <R> R visit(TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class ProjectInfo implements TransformInfo {

    final List<SelectExpression> selectExpressions;
    final LogicalSchema schema;

    ProjectInfo(List<SelectExpression> selectExpressions, LogicalSchema schema) {
      this.selectExpressions = requireNonNull(selectExpressions, "selectExpressions");
      this.schema = requireNonNull(schema, "schema");
    }

    public List<SelectExpression> getSelectExpressions() {
      return selectExpressions;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public <R> R visit(TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }
}

