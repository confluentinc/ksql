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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.udf.AsValue;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.GrammaticalJoiner;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
public class ProjectNode extends PlanNode {

  private final PlanNode source;
  private final ImmutableList<SelectExpression> selectExpressions;
  private final ImmutableMap<ColumnName, ColumnName> aliases;

  public ProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectExpression> projectExpressions,
      final LogicalSchema schema,
      final boolean aliased
  ) {
    super(id, source.getNodeOutputType(), schema, source.getSourceName());

    this.source = requireNonNull(source, "source");

    this.selectExpressions = ImmutableList
        .copyOf(requireNonNull(projectExpressions, "projectExpressions"));

    this.aliases = aliased
        ? buildAliasMapping(projectExpressions)
        : ImmutableMap.of();

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

  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final SchemaKStream<?> stream = getSource().buildStream(builder);

    final List<ColumnName> keyColumnNames = getSchema().key().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    return stream.select(
        keyColumnNames,
        selectExpressions,
        builder.buildNodeContext(getId().toString()),
        builder
    );
  }

  @Override
  public Stream<ColumnName> resolveSelectStar(
      final Optional<SourceName> sourceName
  ) {
    return source.resolveSelectStar(sourceName)
        .map(name -> aliases.getOrDefault(name, name));
  }

  private void validate() {
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

    if (schema.value().size() != selectExpressions.size()) {
      throw new IllegalArgumentException("Error in projection. "
          + "Schema fields and expression list are not compatible.");
    }

    for (int i = 0; i < selectExpressions.size(); i++) {
      final Column column = schema.value().get(i);
      final SelectExpression selectExpression = selectExpressions.get(i);

      if (!column.name().equals(selectExpression.getAlias())) {
        throw new IllegalArgumentException("Mismatch between schema and selects");
      }
    }
  }

  private static ImmutableBiMap<ColumnName, ColumnName> buildAliasMapping(
      final List<SelectExpression> projectExpressions
  ) {
    final ImmutableBiMap.Builder<ColumnName, ColumnName> builder = ImmutableBiMap.builder();

    projectExpressions.stream()
        .filter(se -> se.getExpression() instanceof ColumnReferenceExp)
        .forEach(se -> builder.put(
            ((ColumnReferenceExp) se.getExpression()).getColumnName(),
            se.getAlias()
        ));

    return builder.build();
  }
}
