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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
public abstract class PlanNode {

  private final PlanNodeId id;
  private final DataSourceType nodeOutputType;
  private final LogicalSchema schema;
  private final Optional<SourceName> sourceName;

  protected PlanNode(
      final PlanNodeId id,
      final DataSourceType nodeOutputType,
      final LogicalSchema schema,
      final Optional<SourceName> sourceName
  ) {
    this.id = requireNonNull(id, "id");
    this.nodeOutputType = requireNonNull(nodeOutputType, "nodeOutputType");
    this.schema = requireNonNull(schema, "schema");
    this.sourceName = requireNonNull(sourceName, "sourceName");
  }

  public final PlanNodeId getId() {
    return id;
  }

  public final DataSourceType getNodeOutputType() {
    return nodeOutputType;
  }

  public final LogicalSchema getSchema() {
    return schema;
  }

  public abstract KeyField getKeyField();

  public abstract List<PlanNode> getSources();

  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitPlan(this, context);
  }

  public DataSourceNode getTheSourceNode() {
    if (this instanceof DataSourceNode) {
      return (DataSourceNode) this;
    } else if (!getSources().isEmpty()) {
      return this.getSources().get(0).getTheSourceNode();
    }
    throw new IllegalStateException("No source node in hierarchy");
  }

  protected abstract int getPartitions(KafkaTopicClient kafkaTopicClient);

  public abstract SchemaKStream<?> buildStream(KsqlQueryBuilder builder);

  Optional<SourceName> getSourceName() {
    return sourceName;
  }

  /**
   * Call to resolve an {@link io.confluent.ksql.parser.tree.AllColumns} instance into a
   * corresponding set of columns.
   *
   * @param sourceName the name of the source
   * @param valueOnly {@code false} if key & system columns should be included.
   * @return the list of columns.
   */
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName,
      final boolean valueOnly
  ) {
    return getSources().stream()
        .filter(s -> !sourceName.isPresent() || sourceName.equals(s.getSourceName()))
        .flatMap(s -> s.resolveSelectStar(sourceName, valueOnly));
  }

  /**
   * Called to resolve the supplied {@code expression} into an expression that matches the nodes
   * schema.
   *
   * <p>{@link AggregateNode} and {@link FlatMapNode} replace UDAFs and UDTFs with synthetic column
   * names. Where a select is a UDAF or UDTF this method will return the appropriate synthetic
   * {@link io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp}
   *
   *
   * @param idx the index of the select within the projection.
   * @param expression the expression to resolve.
   * @return the resolved expression.
   */
  public Expression resolveSelect(final int idx, final Expression expression) {
    return expression;
  }
}
