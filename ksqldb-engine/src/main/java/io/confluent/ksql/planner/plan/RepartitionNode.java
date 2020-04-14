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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
public class RepartitionNode extends PlanNode {

  private final PlanNode source;
  private final PartitionBy partitionBy;
  private final KeyField keyField;

  public RepartitionNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final PartitionBy partitionBy,
      final KeyField keyField
  ) {
    super(id, source.getNodeOutputType(), schema, source.getSourceName());
    this.source = requireNonNull(source, "source");
    this.partitionBy = requireNonNull(partitionBy, "partitionBy");
    this.keyField = requireNonNull(keyField, "keyField");
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
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
            partitionBy.getExpression(),
            partitionBy.getAlias(),
            builder.buildNodeContext(getId().toString())
        );
  }

  @VisibleForTesting
  public Expression getPartitionBy() {
    return partitionBy.getExpression();
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitRepartition(this, context);
  }

  @Override
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName,
      final boolean valueOnly
  ) {
    final boolean sourceNameMatches = !sourceName.isPresent() || sourceName.equals(getSourceName());
    if (sourceNameMatches && valueOnly) {
      // Override set of value columns to take into account the repartition:
      return getSchema().withoutMetaAndKeyColsInValue().value().stream().map(Column::name);
    }

    // Set of all columns not changed by a repartition:
    return super.resolveSelectStar(sourceName, valueOnly);
  }
}
