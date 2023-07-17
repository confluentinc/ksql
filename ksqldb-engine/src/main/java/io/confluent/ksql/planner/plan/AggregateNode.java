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

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.analyzer.AggregateAnalysisResult;
import io.confluent.ksql.analyzer.AggregateExpressionRewriter;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.BytesLiteral;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class AggregateNode extends SingleSourcePlanNode implements VerifiableNode {

  private static final String INTERNAL_COLUMN_NAME_PREFIX = "KSQL_INTERNAL_COL_";

  private static final String PREPARE_OP_NAME = "Prepare";
  private static final String AGGREGATION_OP_NAME = "Aggregate";
  private static final String GROUP_BY_OP_NAME = "GroupBy";
  private static final String HAVING_FILTER_OP_NAME = "HavingFilter";
  private static final String PROJECT_OP_NAME = "Project";

  private final GroupBy groupBy;
  private final Optional<WindowExpression> windowExpression;
  private final ImmutableList<Expression> aggregateFunctionArguments;
  private final ImmutableList<FunctionCall> functionList;
  private final ImmutableList<ColumnReferenceExp> requiredColumns;
  private final Optional<Expression> havingExpressions;
  private final ImmutableList<SelectExpression> selectExpressions;
  private final ImmutableList<SelectExpression> finalSelectExpressions;
  private final ValueFormat valueFormat;
  private final LogicalSchema schema;
  private final KsqlConfig ksqlConfig;
  private final ExpressionTypeManager sourceTypeManager;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public AggregateNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final GroupBy groupBy,
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis,
      final AggregateAnalysisResult rewrittenAggregateAnalysis,
      final List<SelectExpression> projectionExpressions,
      final boolean persistentQuery,
      final KsqlConfig ksqlConfig,
      final LogicalSchema sourceSchema
  ) {
    super(id, DataSourceType.KTABLE, Optional.empty(), source);

    this.schema = requireNonNull(schema, "schema");
    this.groupBy = requireNonNull(groupBy, "groupBy");
    this.windowExpression = requireNonNull(analysis, "analysis").getWindowExpression();
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");

    final AggregateExpressionRewriter aggregateExpressionRewriter =
        new AggregateExpressionRewriter(functionRegistry);

    this.aggregateFunctionArguments = ImmutableList
        .copyOf(rewrittenAggregateAnalysis.getAggregateFunctionArguments());
    this.functionList = ImmutableList
        .copyOf(rewrittenAggregateAnalysis.getAggregateFunctions());
    this.requiredColumns = ImmutableList
        .copyOf(rewrittenAggregateAnalysis.getRequiredColumns());
    this.selectExpressions = ImmutableList
        .copyOf(requireNonNull(projectionExpressions, "projectionExpressions"));

    final Set<Expression> groupings = ImmutableSet.copyOf(groupBy.getGroupingExpressions());

    this.finalSelectExpressions = ImmutableList.copyOf(projectionExpressions.stream()
        .map(se -> SelectExpression.of(
            se.getAlias(),
            ExpressionTreeRewriter
                .rewriteWith(aggregateExpressionRewriter::process, se.getExpression())
        ))
        .filter(e -> !persistentQuery || !groupings.contains(e.getExpression()))
        .collect(Collectors.toList()));

    this.havingExpressions = rewrittenAggregateAnalysis.getHavingExpression()
        .map(exp -> ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, exp));

    this.valueFormat = getLeftmostSourceNode()
        .getDataSource()
        .getKsqlTopic()
        .getValueFormat();

    this.sourceTypeManager = new ExpressionTypeManager(sourceSchema, functionRegistry);
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  public List<Expression> getGroupByExpressions() {
    return groupBy.getGroupingExpressions();
  }

  public Optional<WindowExpression> getWindowExpression() {
    return windowExpression;
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {
    final QueryContext.Stacker contextStacker = buildContext.buildNodeContext(getId().toString());
    final SchemaKStream<?> sourceSchemaKStream = getSource().buildStream(buildContext);

    final InternalSchema internalSchema = new InternalSchema(
            requiredColumns,
            aggregateFunctionArguments,
            buildContext.getFunctionRegistry(),
            sourceTypeManager
    );

    final SchemaKStream<?> preSelected = selectRequiredInputColumns(
        sourceSchemaKStream, internalSchema, contextStacker, buildContext);

    final SchemaKGroupedStream grouped = groupBy(contextStacker, preSelected);

    SchemaKTable<?> aggregated = aggregate(grouped, internalSchema, contextStacker);

    aggregated = applyHavingFilter(aggregated, contextStacker);

    return selectRequiredOutputColumns(aggregated, contextStacker, buildContext);
  }

  @Override
  public void validateKeyPresent(final SourceName sinkName) {
    final List<Expression> missing = new ArrayList<>(groupBy.getGroupingExpressions());

    selectExpressions.stream()
        .map(SelectExpression::getExpression)
        .forEach(missing::remove);
    if (!missing.isEmpty()) {
      if (missing.contains(new BytesLiteral(ByteBuffer.wrap(new byte[] {1})))) {
        throw new KsqlException("CREATE TABLE AS SELECT statement does not support "
            + "aggregate function " + functionList +  " without a GROUP BY clause.");
      }
      throwKeysNotIncludedError(sinkName, "grouping expression", missing);
    }
  }

  private SchemaKStream<?> selectRequiredInputColumns(
      final SchemaKStream<?> sourceSchemaKStream,
      final InternalSchema internalSchema,
      final Stacker contextStacker,
      final PlanBuildContext buildContext
  ) {
    final List<ColumnName> keyColumnNames = getSource().getSchema().key().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    return sourceSchemaKStream.select(
        keyColumnNames,
        internalSchema.getAggArgExpansionList(),
        contextStacker.push(PREPARE_OP_NAME),
        buildContext,
        valueFormat.getFormatInfo()
    );
  }

  private SchemaKTable<?> aggregate(
      final SchemaKGroupedStream grouped,
      final InternalSchema internalSchema,
      final Stacker contextStacker
  ) {
    final List<FunctionCall> functions = internalSchema.updateFunctionList(functionList);

    final Stacker aggregationContext = contextStacker.push(AGGREGATION_OP_NAME);

    final List<ColumnName> requiredColumnNames = requiredColumns.stream()
        .map(e -> (UnqualifiedColumnReferenceExp) internalSchema.resolveToInternal(e))
        .map(UnqualifiedColumnReferenceExp::getColumnName)
        .collect(Collectors.toList());

    return grouped.aggregate(
        requiredColumnNames,
        functions,
        windowExpression,
        valueFormat.getFormatInfo(),
        aggregationContext
    );
  }

  private SchemaKTable<?> applyHavingFilter(
      final SchemaKTable<?> aggregated,
      final Stacker contextStacker
  ) {
    return havingExpressions.isPresent()
        ? aggregated.filter(havingExpressions.get(), contextStacker.push(HAVING_FILTER_OP_NAME))
        : aggregated;
  }

  private SchemaKStream<?> selectRequiredOutputColumns(
      final SchemaKTable<?> aggregated,
      final Stacker contextStacker,
      final PlanBuildContext buildContext
  ) {
    final List<ColumnName> keyColumnNames = getSchema().key().stream()
        .map(Column::name)
        .collect(Collectors.toList());

    return aggregated.select(
        keyColumnNames,
        finalSelectExpressions,
        contextStacker.push(PROJECT_OP_NAME),
        buildContext,
        valueFormat.getFormatInfo()
    );
  }

  private SchemaKGroupedStream groupBy(
      final Stacker contextStacker,
      final SchemaKStream<?> preSelected
  ) {
    return preSelected.groupBy(
        valueFormat.getFormatInfo(),
        groupBy.getGroupingExpressions(),
        contextStacker.push(GROUP_BY_OP_NAME)
    );
  }

  private static class InternalSchema {

    private final List<SelectExpression> aggArgExpansions = new ArrayList<>();
    private final Map<String, ColumnName> expressionToInternalColumnName = new HashMap<>();
    private final FunctionRegistry functionRegistry;
    private final ExpressionTypeManager sourceTypeManager;

    InternalSchema(
        final List<ColumnReferenceExp> requiredColumns,
        final List<Expression> aggregateFunctionArguments,
        final FunctionRegistry functionRegistry,
        final ExpressionTypeManager sourceTypeManager
    ) {
      collectAggregateArgExpressions(requiredColumns);
      collectAggregateArgExpressions(aggregateFunctionArguments);
      this.functionRegistry = functionRegistry;
      this.sourceTypeManager = sourceTypeManager;
    }

    private void collectAggregateArgExpressions(
        final Collection<? extends Expression> expressions
    ) {
      for (final Expression expression : expressions) {
        final String sql = expression.toString();
        if (expressionToInternalColumnName.containsKey(sql)) {
          continue;
        }

        final ColumnName internalName = expression instanceof ColumnReferenceExp
            ? ((ColumnReferenceExp) expression).getColumnName()
            : ColumnName.of(INTERNAL_COLUMN_NAME_PREFIX + aggArgExpansions.size());

        aggArgExpansions.add(SelectExpression.of(internalName, expression));
        expressionToInternalColumnName.put(sql, internalName);
      }
    }

    /**
     * Return the aggregate function arguments based on the internal expressions.
     *
     * <p>Aggregate functions can take any number of arguments.
     *
     * @param functionName The name of the aggregate function.
     * @param params The list of parameters for the aggregate function.
     * @return The list of arguments based on the internal expressions for the aggregate function.
     */
    List<Expression> updateArgsExpressionList(final FunctionName functionName,
                                              final List<Expression> params) {
      if (params.isEmpty()) {
        return ImmutableList.of();
      }

      final int numInitArgs = functionRegistry.getAggregateFactory(functionName).getFunction(
              params.stream()
                      .map(sourceTypeManager::getExpressionSqlType)
                      .collect(Collectors.toList())
      ).initArgs;
      final int numColArgs = params.size() - numInitArgs;

      final List<Expression> internalParams = new ArrayList<>(params.size());

      internalParams.addAll(params.subList(0, numColArgs).stream()
              .map(this::resolveToInternal)
              .collect(Collectors.toList()));
      internalParams.addAll(params.subList(numColArgs, params.size()));

      return internalParams;
    }

    List<FunctionCall> updateFunctionList(final ImmutableList<FunctionCall> functions) {
      return functions.stream()
          .map(fc -> new FunctionCall(fc.getName(), updateArgsExpressionList(
                  fc.getName(),
                  fc.getArguments()
          )))
          .collect(Collectors.toList());
    }

    List<SelectExpression> getAggArgExpansionList() {
      return aggArgExpansions;
    }

    private Expression resolveToInternal(final Expression exp) {
      final ColumnName name = expressionToInternalColumnName.get(exp.toString());
      if (name != null) {
        return new UnqualifiedColumnReferenceExp(
            exp.getLocation(),
            name);
      }

      return ExpressionTreeRewriter.rewriteWith(new ResolveToInternalRewriter()::process, exp);
    }

    private final class ResolveToInternalRewriter
        extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

      private ResolveToInternalRewriter() {
        super(Optional.empty());
      }

      @Override
      public Optional<Expression> visitUnqualifiedColumnReference(
          final UnqualifiedColumnReferenceExp node,
          final Context<Void> context
      ) {
        // internal names are source-less
        final ColumnName name = expressionToInternalColumnName.get(node.toString());
        if (name != null) {
          return Optional.of(
              new UnqualifiedColumnReferenceExp(
                  node.getLocation(),
                  name));
        }

        final boolean isAggregate = ColumnNames.isAggregate(node.getColumnName());
        final boolean windowBounds = SystemColumns.isWindowBound(node.getColumnName());

        if (isAggregate && windowBounds) {
          throw new KsqlException("Window bound " + node + " is not available as a parameter "
              + "to aggregate functions");
        }

        if (!isAggregate && !windowBounds) {
          throw new KsqlException("Unknown source column: " + node);
        }

        return Optional.of(node);
      }
    }
  }
}
