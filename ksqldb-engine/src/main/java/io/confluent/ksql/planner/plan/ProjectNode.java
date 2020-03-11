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
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;

@Immutable
public class ProjectNode extends PlanNode {

  private final PlanNode source;
  private final KeyField keyField;

  public ProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectExpression> projectExpressions,
      final LogicalSchema schema,
      final Optional<ColumnName> keyFieldName
  ) {
    super(id, source.getNodeOutputType(), schema, projectExpressions);

    this.source = requireNonNull(source, "source");
    this.keyField = KeyField.of(requireNonNull(keyFieldName, "keyFieldName"))
        .validateKeyExistsIn(schema);

    validate();
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

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .select(
            getSelectExpressions(),
            builder.buildNodeContext(getId().toString()),
            builder
        );
  }

  private void validate() {
    if (getSchema().value().size() != getSelectExpressions().size()) {
      throw new KsqlException("Error in projection. Schema fields and expression list are not "
          + "compatible.");
    }

    for (int i = 0; i < getSelectExpressions().size(); i++) {
      final Column column = getSchema().value().get(i);
      final SelectExpression selectExpression = getSelectExpressions().get(i);

      if (!column.name().equals(selectExpression.getAlias())) {
        throw new IllegalArgumentException("Mismatch between schema and selects");
      }
    }
  }
}
