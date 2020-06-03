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

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;

public abstract class RepartitionNode extends PlanNode {

  private final PlanNode source;
  private final Expression partitionBy;
  private final LogicalSchema schema;

  public RepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Expression partitionBy
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName());
    this.schema = requireNonNull(schema, "schema");
    this.source = requireNonNull(source, "source");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return source.buildStream(builder)
        .selectKey(
            partitionBy,
            builder.buildNodeContext(getId().toString())
        );
  }

  @VisibleForTesting
  public Expression getPartitionBy() {
    return partitionBy;
  }
}
