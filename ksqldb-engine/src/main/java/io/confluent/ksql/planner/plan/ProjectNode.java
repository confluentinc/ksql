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
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.udf.AsValue;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ProjectNode extends PlanNode {

  private final PlanNode source;

  public ProjectNode(
      final PlanNodeId id,
      final PlanNode source
  ) {
    super(id, source.getNodeOutputType(), source.getSourceName());

    this.source = requireNonNull(source, "source");
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  public abstract List<SelectExpression> getSelectExpressions();

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final SchemaKStream<?> stream = getSource().buildStream(builder);

    final List<ColumnName> keyColumnNames = getSchema().key().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    return stream.select(
        keyColumnNames,
        getSelectExpressions(),
        builder.buildNodeContext(getId().toString()),
        builder
    );
  }

  protected void validate() {
    final LogicalSchema schema = getSchema();

    if (schema.key().size() > 1) {
      final String keys = GrammaticalJoiner.and().join(schema.key().stream().map(Column::name));
      throw new KsqlException("The projection contains the key column more than once: " + keys + "."
          + System.lineSeparator()
          + "Each key column must only be in the projection once. "
          + "If you intended to copy the key into the value, then consider using the "
          + AsValue.NAME + " function to indicate which key reference should be copied."
      );
    }

    if (schema.value().isEmpty()) {
      throw new KsqlException("The projection contains no value columns.");
    }

    final List<SelectExpression> selectExpressions = getSelectExpressions();

    if (schema.value().size() != selectExpressions.size()) {
      throw new IllegalArgumentException("Error in projection. "
          + "Schema fields and expression list size mismatch.");
    }

    for (int i = 0; i < selectExpressions.size(); i++) {
      final Column column = schema.value().get(i);
      final SelectExpression selectExpression = selectExpressions.get(i);

      if (!column.name().equals(selectExpression.getAlias())) {
        throw new IllegalArgumentException("Mismatch between schema and selects");
      }
    }
  }
}
