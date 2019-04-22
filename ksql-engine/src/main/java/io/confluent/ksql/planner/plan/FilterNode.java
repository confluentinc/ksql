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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.physical.KsqlQueryBuilder;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KafkaTopicClient;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

@Immutable
public class FilterNode extends PlanNode {

  private final PlanNode source;
  private final Expression predicate;
  private final Schema schema;

  @JsonCreator
  public FilterNode(@JsonProperty("id") final PlanNodeId id,
                    @JsonProperty("source") final PlanNode source,
                    @JsonProperty("predicate") final Expression predicate) {
    super(id, source.getNodeOutputType());

    this.source = source;
    this.schema = source.getSchema();
    this.predicate = predicate;
  }

  @JsonProperty("predicate")
  public Expression getPredicate() {
    return predicate;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public Optional<Field> getKeyField() {
    return source.getKeyField();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @JsonProperty("source")
  public PlanNode getSource() {
    return source;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .filter(
            getPredicate(),
            builder.buildNodeContext(getId()),
            builder.getProcessingLogContext()
        );
  }
}
