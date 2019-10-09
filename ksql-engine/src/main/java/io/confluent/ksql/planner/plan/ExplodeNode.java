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
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ExplodeNode extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema schema;

  public ExplodeNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema
  ) {
    super(id, source.getNodeOutputType());

    this.source = requireNonNull(source, "source");
    this.schema = requireNonNull(schema, "schema");

  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public KeyField getKeyField() {
    return null;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitExplode(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return null;
  }

}
