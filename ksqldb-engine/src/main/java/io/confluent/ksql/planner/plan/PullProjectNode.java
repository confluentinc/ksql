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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.RewrittenAnalysis;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.interpreter.InterpretedExpressionFactory;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.planner.Projection;
import io.confluent.ksql.planner.PullPlannerOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The projection of a Pull query.
 *
 * <p>There are 3 schemas the node is handling, the input schema, the intermediate schema used
 * for codegen and the output schema.
 * <ul>
 * <li>The input is the schema of the child node
 *
 * <li>CodeGen is used only if the projection is not SELECT *. For CodeGen, an intermediate schema
 * is created as follows:
 * Check if projection contains system or key columns. If not, the intermediate schema
 * is the input schema. If there are any of these columns, the input schema is extended by copying
 * the key and system columns (rowtime, windwostart and windowend) into the value of the schema.
 *
 * <li>For the output schema, if the projection is SELECT *, add windowstart and windowend to key
 * columns and keep value columns the same as input. If projection is not SELECT *,
 * then process each select and if it is a key or windowstart and windowend add them to the key
 * part else add them to the value part.
 * </ul>
 */
public class PullProjectNode extends ProjectNode {

  private final Projection projection;
  private final ImmutableList<SelectExpression> selectExpressions;
  private final LogicalSchema outputSchema;
  private final LogicalSchema intermediateSchema;
  private final List<ExpressionEvaluator> compiledSelectExpressions;
  private final RewrittenAnalysis analysis;
  private final PullPlannerOptions pullPlannerOptions;
  private final boolean isSelectStar;
  private final boolean addAdditionalColumnsToIntermediateSchema;

  public PullProjectNode(
      final PlanNodeId id,
      final PlanNode source,
      final List<SelectItem> selectItems,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final RewrittenAnalysis analysis,
      final boolean isWindowed,
      final PullPlannerOptions pullPlannerOptions
  ) {
    super(id, source);
    this.projection = Projection.of(selectItems);
    this.analysis = Objects.requireNonNull(analysis, "analysis");
    this.pullPlannerOptions = Objects.requireNonNull(pullPlannerOptions, "pullPlannerOptions");
    this.selectExpressions = ImmutableList.copyOf(SelectionUtil
        .buildSelectExpressions(getSource(), projection.selectItems(), Optional.empty()));
    this.isSelectStar = isSelectStar();
    this.addAdditionalColumnsToIntermediateSchema = shouldAddAdditionalColumnsInSchema();
    this.outputSchema = buildOutputSchema(metaStore);
    this.intermediateSchema = PullLogicalPlanUtil.buildIntermediateSchema(
          source.getSchema(),
          addAdditionalColumnsToIntermediateSchema,
          isWindowed
      );
    this.compiledSelectExpressions = isSelectStar
        ? Collections.emptyList()
        : selectExpressions
        .stream()
        .map(selectExpression ->
            getExpressionEvaluator(
                selectExpression.getExpression(), intermediateSchema, metaStore, ksqlConfig,
                pullPlannerOptions)
        )
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public LogicalSchema getSchema() {
    return outputSchema;
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return selectExpressions;
  }

  public List<ExpressionEvaluator> getCompiledSelectExpressions() {
    if (isSelectStar) {
      throw new IllegalStateException("Select expressions aren't compiled for select star");
    }
    return compiledSelectExpressions;
  }

  public LogicalSchema getIntermediateSchema() {
    return intermediateSchema;
  }

  public boolean getIsSelectStar() {
    return isSelectStar;
  }

  public boolean getAddAdditionalColumnsToIntermediateSchema() {
    return addAdditionalColumnsToIntermediateSchema;
  }

  /**
   * Builds the output schema of the project node.
   * The output schema comprises of exactly the columns that appear in the SELECT clause of the
   * query.
   * @param metaStore the metastore
   * @return the project node's output schema
   */
  private LogicalSchema buildOutputSchema(final MetaStore metaStore) {
    final LogicalSchema outputSchema;
    final LogicalSchema parentSchema = getSource().getSchema();
    final boolean isWindowed = analysis
        .getFrom()
        .getDataSource()
        .getKsqlTopic()
        .getKeyFormat().isWindowed();

    if (isSelectStar()) {
      outputSchema = buildPullQuerySelectStarSchema(
          parentSchema.withoutPseudoAndKeyColsInValue(), isWindowed);
    } else {
      final List<SelectExpression> projects = projection.selectItems().stream()
          .map(SingleColumn.class::cast)
          .map(si -> SelectExpression
              .of(si.getAlias().orElseThrow(IllegalStateException::new), si.getExpression()))
          .collect(Collectors.toList());

      outputSchema = selectOutputSchema(metaStore, projects, isWindowed);
    }
    return outputSchema;
  }

  /**
   * Checks whether the intermediate schema should be extended with system and key columns.
   * @return true if the intermediate schema should be extended
   */
  private boolean shouldAddAdditionalColumnsInSchema() {

    final boolean hasSystemColumns = analysis.getSelectColumnNames().stream().anyMatch(
        SystemColumns::isSystemColumn
    );

    final boolean hasKeyColumns = analysis.getSelectColumnNames().stream().anyMatch(cn ->
        getSource().getSchema().isKeyColumn(cn)
    );

    // Select * also requires keys, in case it's not explicitly mentioned
    return hasSystemColumns || hasKeyColumns || isSelectStar;
  }

  private boolean isSelectStar() {
    final boolean someStars = projection.selectItems().stream()
        .anyMatch(s -> s instanceof AllColumns);

    if (someStars && projection.selectItems().size() != 1) {
      throw new KsqlException("Pull queries only support wildcards in the projects "
                                  + "if they are the only expression");
    }

    return someStars;
  }

  private LogicalSchema buildPullQuerySelectStarSchema(
      final LogicalSchema schema,
      final boolean windowed
  ) {
    final Builder builder = LogicalSchema.builder()
        .keyColumns(schema.key());

    if (windowed) {
      builder.keyColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.BIGINT);
      builder.keyColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.BIGINT);
    }

    return builder
        .valueColumns(schema.value())
        .build();
  }

  private LogicalSchema selectOutputSchema(
      final MetaStore metaStore,
      final List<SelectExpression> selectExpressions,
      final boolean isWindowed
  ) {
    final Builder schemaBuilder = LogicalSchema.builder();
    final LogicalSchema parentSchema = getSource().getSchema();

    // Copy meta & key columns into the value schema as SelectValueMapper expects it:
    final LogicalSchema schema = parentSchema
        .withPseudoAndKeyColsInValue(isWindowed);

    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(schema, metaStore);

    for (final SelectExpression select : selectExpressions) {
      final SqlType type = expressionTypeManager.getExpressionSqlType(select.getExpression());

      if (parentSchema.isKeyColumn(select.getAlias())
          || select.getAlias().equals(SystemColumns.WINDOWSTART_NAME)
          || select.getAlias().equals(SystemColumns.WINDOWEND_NAME)
      ) {
        schemaBuilder.keyColumn(select.getAlias(), type);
      } else {
        schemaBuilder.valueColumn(select.getAlias(), type);
      }
    }
    return schemaBuilder.build();
  }

  private static ExpressionEvaluator getExpressionEvaluator(
      final Expression expression,
      final LogicalSchema schema,
      final MetaStore metaStore,
      final KsqlConfig ksqlConfig,
      final PullPlannerOptions pullPlannerOptions) {

    if (pullPlannerOptions.getInterpreterEnabled()) {
      return InterpretedExpressionFactory.create(
          expression,
          schema,
          metaStore,
          ksqlConfig
      );
    } else {
      return CodeGenRunner.compileExpression(
          expression,
          "Select",
          schema,
          ksqlConfig,
          metaStore
      );
    }
  }
}
