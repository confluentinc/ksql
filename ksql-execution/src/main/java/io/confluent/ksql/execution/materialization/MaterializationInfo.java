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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Pojo for passing around information about materialization of a query's state store
 */
@Immutable
public final class MaterializationInfo {

  private final String stateStoreName;
  private final LogicalSchema stateStoreSchema;
  private final List<TransformInfo> transforms;
  private final LogicalSchema schema;

  /**
   * Create instance.
   *
   * @param stateStoreName the name of the state store
   * @param stateStoreSchema the schema of the data in the state store
   * @param transforms a list of transformations to be applied to the data in the store
   * @param schema the schema of the result of applying transformations to the data in the store
   * @return instance.
   */
  public static MaterializationInfo of(
      final String stateStoreName,
      final LogicalSchema stateStoreSchema,
      final List<TransformInfo> transforms,
      final LogicalSchema schema
  ) {
    return new MaterializationInfo(stateStoreName, stateStoreSchema, transforms, schema);
  }

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
      final String stateStoreName,
      final LogicalSchema stateStoreSchema,
      final List<TransformInfo> transforms,
      final LogicalSchema schema
  ) {
    this.stateStoreName = requireNonNull(stateStoreName, "stateStoreName");
    this.stateStoreSchema = requireNonNull(stateStoreSchema, "stateStoreSchema");
    this.transforms = requireNonNull(transforms, "transforms");
    this.schema = requireNonNull(schema, "schema");
  }

  public static class Builder {
    private final String stateStoreName;
    private final LogicalSchema stateStoreSchema;
    private final List<TransformInfo> transforms;
    private LogicalSchema schema;

    public Builder(final String stateStoreName, final LogicalSchema stateStoreSchema) {
      this.stateStoreName = Objects.requireNonNull(stateStoreName, "stateStoreName");
      this.stateStoreSchema = Objects.requireNonNull(stateStoreSchema, "stateStoreSchema");
      this.transforms = new LinkedList<>();
      this.schema = stateStoreSchema;
    }

    public Builder mapAggregates(final AggregatesInfo aggregatesInfo, final LogicalSchema schema) {
      transforms.add(new AggregateMapInfo(aggregatesInfo));
      this.schema = schema;
      return this;
    }

    public Builder mapValues(
        final List<SelectExpression> selectExpressions,
        final LogicalSchema schema) {
      transforms.add(new SelectMapperInfo(selectExpressions, this.schema));
      this.schema = schema;
      return this;
    }

    public Builder filter(final Expression filterExpression) {
      transforms.add(new SqlPredicateInfo(filterExpression, schema));
      return this;
    }

    public MaterializationInfo build() {
      return new MaterializationInfo(stateStoreName, stateStoreSchema, transforms, schema);
    }
  }

  public interface TransformInfo {
    <R> R visit(TransformVisitor<R> visitor);
  }

  public interface TransformVisitor<R> {
    R visit(AggregateMapInfo mapInfo);

    R visit(SqlPredicateInfo info);

    R visit(SelectMapperInfo info);
  }

  public static class AggregateMapInfo implements TransformInfo {
    final AggregatesInfo info;

    AggregateMapInfo(final AggregatesInfo info) {
      this.info = Objects.requireNonNull(info, "info");
    }

    public AggregatesInfo getInfo() {
      return info;
    }

    public <R> R visit(final TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class SqlPredicateInfo implements TransformInfo {
    final Expression filterExpression;
    final LogicalSchema schema;

    SqlPredicateInfo(final Expression filterExpression, final LogicalSchema schema) {
      this.filterExpression = Objects.requireNonNull(filterExpression, "filterExpression");
      this.schema = Objects.requireNonNull(schema, "schema");
    }

    public Expression getFilterExpression() {
      return filterExpression;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public <R> R visit(final TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }

  public static class SelectMapperInfo implements TransformInfo {
    final List<SelectExpression> selectExpressions;
    final LogicalSchema schema;

    SelectMapperInfo(final List<SelectExpression> selectExpressions, final LogicalSchema schema) {
      this.selectExpressions = Objects.requireNonNull(selectExpressions, "selectExpressions");
      this.schema = Objects.requireNonNull(schema, "schema");
    }

    public List<SelectExpression> getSelectExpressions() {
      return selectExpressions;
    }

    public LogicalSchema getSchema() {
      return schema;
    }

    @Override
    public <R> R visit(final TransformVisitor<R> visitor) {
      return visitor.visit(this);
    }
  }
}

