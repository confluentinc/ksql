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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;

@Immutable
public abstract class PlanNode {

  private final PlanNodeId id;
  private final DataSourceType nodeOutputType;
  private final LogicalSchema schema;
  private final ImmutableList<SelectExpression> selectExpressions;

  protected PlanNode(
      final PlanNodeId id,
      final DataSourceType nodeOutputType,
      final LogicalSchema schema,
      final List<SelectExpression> selectExpressions
  ) {
    this.id = requireNonNull(id, "id");
    this.nodeOutputType = requireNonNull(nodeOutputType, "nodeOutputType");
    this.schema = requireNonNull(schema, "schema");
    this.selectExpressions = ImmutableList
        .copyOf(requireNonNull(selectExpressions, "projectExpressions"));
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

  public final List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public abstract KeyField getKeyField();

  public abstract List<PlanNode> getSources();

  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitPlan(this, context);
  }

  public DataSourceNode getTheSourceNode() {
    if (this instanceof DataSourceNode) {
      return (DataSourceNode) this;
    } else if (this.getSources() != null && !this.getSources().isEmpty()) {
      return this.getSources().get(0).getTheSourceNode();
    }
    return null;
  }

  protected abstract int getPartitions(KafkaTopicClient kafkaTopicClient);

  public abstract SchemaKStream<?> buildStream(KsqlQueryBuilder builder);
}
