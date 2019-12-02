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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Objects;

@Immutable
public class RepartitionNode extends PlanNode {

  private final PlanNode source;
  private final ColumnRef partitionBy;
  private final KeyField keyField;

  public RepartitionNode(PlanNodeId id, PlanNode source, ColumnRef partitionBy, KeyField keyField) {
    super(id, source.getNodeOutputType());
    final SourceName alias = source.getTheSourceNode().getAlias();
    this.source = Objects.requireNonNull(source, "source");
    this.partitionBy = Objects.requireNonNull(partitionBy, "partitionBy").withSource(alias);
    this.keyField = Objects.requireNonNull(keyField, "keyField");
  }

  @Override
  public LogicalSchema getSchema() {
    return source.getSchema();
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
  public List<SelectExpression> getSelectExpressions() {
    return source.getSelectExpressions();
  }

  @Override
  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(KsqlQueryBuilder builder) {
    return source.buildStream(builder)
        .selectKey(partitionBy, builder.buildNodeContext(getId().toString()));
  }
}
