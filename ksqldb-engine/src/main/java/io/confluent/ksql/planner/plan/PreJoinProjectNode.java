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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.planner.RequiredColumns;
import io.confluent.ksql.planner.RequiredColumns.Builder;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Logical plan node that prepends a source's alias to the start of each column name.
 *
 * <p>This aliasing avoids column name clashes between the sources within a join.
 */
public class PreJoinProjectNode extends ProjectNode implements JoiningNode {

  private final ImmutableList<SelectExpression> selectExpressions;
  private final ImmutableBiMap<ColumnName, ColumnName> aliases;
  private final LogicalSchema schema;
  private final Optional<JoiningNode> joiningSource;

  public PreJoinProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final SourceName alias
  ) {
    super(id, source);

    this.selectExpressions = ImmutableList.copyOf(buildSelectExpressions(
        alias,
        source.getSchema()
    ));
    this.aliases = buildAliasMapping(selectExpressions);
    this.schema = buildSchema(alias, source.getSchema());
    if (source instanceof JoiningNode) {
      this.joiningSource = Optional.of((JoiningNode) source);
    } else {
      if (!(source instanceof DataSourceNode)) {
        throw new IllegalStateException(
            "PreJoinProjectNode preceded by non-DataSourceNode non-JoiningNode: "
                + source.getClass());
      }
      this.joiningSource = Optional.empty();
    }
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  @Override
  public Optional<KeyFormat> getPreferredKeyFormat() {
    if (joiningSource.isPresent()) {
      return joiningSource.get().getPreferredKeyFormat();
    }

    final KeyFormat sourceKeyFormat =
        Iterators.getOnlyElement(getSourceNodes().iterator())
            .getDataSource().getKsqlTopic().getKeyFormat();

    return Optional.of(sourceKeyFormat);
  }

  @Override
  public void setKeyFormat(final KeyFormat format) {
    joiningSource.ifPresent(source -> source.setKeyFormat(format));
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

  private static LogicalSchema buildSchema(
      final SourceName alias,
      final LogicalSchema parentSchema
  ) {
    final LogicalSchema.Builder builder = LogicalSchema.builder();

    parentSchema.columns()
        .forEach(c -> {
          final ColumnName aliasedName = ColumnNames.generatedJoinColumnAlias(alias, c.name());

          if (c.namespace() == Namespace.KEY) {
            builder.keyColumn(aliasedName, c.type());
          } else {
            builder.valueColumn(aliasedName, c.type());
          }
        });

    return builder.build();
  }

  private static List<SelectExpression> buildSelectExpressions(
      final SourceName alias,
      final LogicalSchema schema
  ) {
    return schema.value().stream()
        .map(c -> SelectExpression.of(
            ColumnNames.generatedJoinColumnAlias(alias, c.name()),
            new UnqualifiedColumnReferenceExp(c.name()))
        ).collect(Collectors.toList());
  }
}
