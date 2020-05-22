/*
 * Copyright 2020 Confluent Inc.
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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.planner.RequiredColumns.Builder;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
public class PreJoinProjectNode extends ProjectNode {

  private final ImmutableList<SelectExpression> selectExpressions;
  private final ImmutableBiMap<ColumnName, ColumnName> aliases;
  private final LogicalSchema schema;

  public PreJoinProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectExpression> projectExpressions,
      final LogicalSchema schema
  ) {
    super(id, source);

    this.selectExpressions = ImmutableList
        .copyOf(requireNonNull(projectExpressions, "projectExpressions"));
    this.aliases = buildAliasMapping(projectExpressions);
    this.schema = requireNonNull(schema, "schema");

    validate();
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
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
    return getSource().resolveSelectStar(sourceName)
        .map(name -> aliases.getOrDefault(name, name));
  }

  @Override
  protected Set<ColumnReferenceExp> validateColumns(final RequiredColumns requiredColumns) {
    final List<? extends ColumnReferenceExp> aliased = requiredColumns.get().stream()
        .filter(columnRef -> columnRef instanceof UnqualifiedColumnReferenceExp)
        .filter(columnRef -> aliases.inverse().containsKey(columnRef.getColumnName()))
        .collect(Collectors.toList());

    final Builder builder = requiredColumns.asBuilder();

    aliased.forEach(columnRef -> {
      builder.remove(columnRef);
      builder.add(new UnqualifiedColumnReferenceExp(
          columnRef.getLocation(),
          aliases.inverse().get(columnRef.getColumnName())
      ));
    });

    return super.validateColumns(builder.build());
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
