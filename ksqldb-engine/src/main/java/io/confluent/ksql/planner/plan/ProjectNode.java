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
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KsqlException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Immutable
public class ProjectNode extends PlanNode {

  private final PlanNode source;
  private final ImmutableList<SelectExpression> projectExpressions;
  private final KeyField keyField;
  private final ImmutableMap<ColumnName, ImmutableList<ColumnName>> aliases;

  public ProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectExpression> projectExpressions,
      final LogicalSchema schema,
      final Optional<ColumnName> keyFieldName
  ) {
    super(id, source.getNodeOutputType(), schema, source.getSourceName());

    this.source = requireNonNull(source, "source");
    this.projectExpressions = ImmutableList.copyOf(
        requireNonNull(projectExpressions, "projectExpressions")
    );
    this.keyField = KeyField.of(requireNonNull(keyFieldName, "keyFieldName"))
        .validateKeyExistsIn(schema);
    this.aliases = buildAliasMapping(projectExpressions);

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

  public List<SelectExpression> getSelectExpressions() {
    return projectExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    return getSource().buildStream(builder)
        .select(
            projectExpressions,
            builder.buildNodeContext(getId().toString()),
            builder
        );
  }

  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName,
      final boolean valueOnly
  ) {
    return source.resolveSelectStar(sourceName, valueOnly)
        .map(name -> aliases.getOrDefault(name, ImmutableList.of()))
        .flatMap(Collection::stream);
  }

  private void validate() {
    if (getSchema().value().size() != projectExpressions.size()) {
      throw new KsqlException("Error in projection. Schema fields and expression list are not "
          + "compatible.");
    }

    for (int i = 0; i < projectExpressions.size(); i++) {
      final Column column = getSchema().value().get(i);
      final SelectExpression selectExpression = projectExpressions.get(i);

      if (!column.name().equals(selectExpression.getAlias())) {
        throw new IllegalArgumentException("Mismatch between schema and selects");
      }
    }
  }

  private static ImmutableMap<ColumnName, ImmutableList<ColumnName>> buildAliasMapping(
      final List<SelectExpression> projectExpressions
  ) {
    final Map<ColumnName, ImmutableList.Builder<ColumnName>> aliases = new HashMap<>();

    projectExpressions.stream()
        .filter(se -> se.getExpression() instanceof ColumnReferenceExp)
        .forEach(se -> aliases.computeIfAbsent(
            ((ColumnReferenceExp) se.getExpression()).getColumnName(),
            k -> ImmutableList.builder())
            .add(se.getAlias()));

    final ImmutableMap.Builder<ColumnName, ImmutableList<ColumnName>> builder =
        ImmutableMap.builder();

    aliases.forEach((k, v) -> builder.put(k, v.build()));

    return builder.build();
  }
}
