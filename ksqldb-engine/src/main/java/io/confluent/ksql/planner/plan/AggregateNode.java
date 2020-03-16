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
import io.confluent.ksql.analyzer.AggregateAnalysisResult;
import io.confluent.ksql.analyzer.AggregateExpressionRewriter;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class AggregateNode extends PlanNode {

  private static final String INTERNAL_COLUMN_NAME_PREFIX = "KSQL_INTERNAL_COL_";

  private static final String PREPARE_OP_NAME = "Prepare";
  private static final String AGGREGATION_OP_NAME = "Aggregate";
  private static final String GROUP_BY_OP_NAME = "GroupBy";
  private static final String HAVING_FILTER_OP_NAME = "HavingFilter";
  private static final String PROJECT_OP_NAME = "Project";

  private final PlanNode source;
  private final KeyField keyField;
  private final ImmutableList<Expression> groupByExpressions;
  private final Optional<WindowExpression> windowExpression;
  private final ImmutableList<Expression> aggregateFunctionArguments;
  private final ImmutableList<FunctionCall> functionList;
  private final ImmutableList<ColumnReferenceExp> requiredColumns;
  private final Optional<Expression> havingExpressions;
  private final ImmutableList<SelectExpression> finalSelectExpressions;

  public AggregateNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final Optional<ColumnName> keyFieldName,
      final List<Expression> groupByExpressions,
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis,
      final AggregateAnalysisResult rewrittenAggregateAnalysis,
      final List<SelectExpression> projectionExpressions
  ) {
    super(id, DataSourceType.KTABLE, schema, Optional.empty());

    this.source = requireNonNull(source, "source");
    this.groupByExpressions = ImmutableList
        .copyOf(requireNonNull(groupByExpressions, "groupByExpressions"));
    this.windowExpression = requireNonNull(analysis, "analysis").getWindowExpression();

    final AggregateExpressionRewriter aggregateExpressionRewriter =
        new AggregateExpressionRewriter(functionRegistry);

    this.aggregateFunctionArguments = ImmutableList
        .copyOf(rewrittenAggregateAnalysis.getAggregateFunctionArguments());
    this.functionList = ImmutableList
        .copyOf(rewrittenAggregateAnalysis.getAggregateFunctions());
    this.requiredColumns = ImmutableList
        .copyOf(rewrittenAggregateAnalysis.getRequiredColumns());
    this.finalSelectExpressions = ImmutableList.copyOf(projectionExpressions.stream()
        .map(se -> SelectExpression.of(
            se.getAlias(),
            ExpressionTreeRewriter
                .rewriteWith(aggregateExpressionRewriter::process, se.getExpression())
        ))
        .collect(Collectors.toList()));
    this.havingExpressions = rewrittenAggregateAnalysis.getHavingExpression()
        .map(exp -> ExpressionTreeRewriter.rewriteWith(aggregateExpressionRewriter::process, exp));
    this.keyField = KeyField.of(requireNonNull(keyFieldName, "keyFieldName"))
        .validateKeyExistsIn(schema);
  }

  @Override
  public KeyField getKeyField() {
    return keyField;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  public List<Expression> getGroupByExpressions() {
    return groupByExpressions;
  }

  public Optional<WindowExpression> getWindowExpression() {
    return windowExpression;
  }

  public List<FunctionCall> getFunctionCalls() {
    return functionList;
  }

  public List<ColumnReferenceExp> getRequiredColumns() {
    return requiredColumns;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitAggregate(this, context);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {
    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());
    final DataSourceNode streamSourceNode = getTheSourceNode();
    final SchemaKStream<?> sourceSchemaKStream = getSource().buildStream(builder);

    // Pre aggregate computations
    final InternalSchema internalSchema = new InternalSchema(
        getSchema(),
        requiredColumns,
        aggregateFunctionArguments
    );

    final SchemaKStream<?> aggregateArgExpanded = sourceSchemaKStream.select(
        internalSchema.getAggArgExpansionList(),
        contextStacker.push(PREPARE_OP_NAME),
        builder
    );

    final QueryContext.Stacker groupByContext = contextStacker.push(GROUP_BY_OP_NAME);

    final ValueFormat valueFormat = streamSourceNode
        .getDataSource()
        .getKsqlTopic()
        .getValueFormat();

    final List<Expression> internalGroupByColumns = internalSchema.resolveGroupByExpressions(
        groupByExpressions,
        aggregateArgExpanded
    );

    final SchemaKGroupedStream schemaKGroupedStream = aggregateArgExpanded.groupBy(
        valueFormat,
        internalGroupByColumns,
        groupByContext
    );

    final List<FunctionCall> functionsWithInternalIdentifiers = functionList.stream()
        .map(
            fc -> new FunctionCall(
                fc.getName(),
                internalSchema.getInternalArgsExpressionList(fc.getArguments())
            )
        )
        .collect(Collectors.toList());

    final QueryContext.Stacker aggregationContext = contextStacker.push(AGGREGATION_OP_NAME);

    final List<ColumnName> requiredColumnNames = requiredColumns.stream()
        .map(e -> (UnqualifiedColumnReferenceExp) internalSchema.resolveToInternal(e))
        .map(UnqualifiedColumnReferenceExp::getColumnName)
        .collect(Collectors.toList());

    SchemaKTable<?> aggregated = schemaKGroupedStream.aggregate(
        requiredColumnNames,
        functionsWithInternalIdentifiers,
        windowExpression,
        valueFormat,
        aggregationContext
    );

    final Optional<Expression> havingExpression = havingExpressions
        .map(internalSchema::resolveToInternal);

    if (havingExpression.isPresent()) {
      aggregated = aggregated.filter(
          havingExpression.get(),
          contextStacker.push(HAVING_FILTER_OP_NAME)
      );
    }

    final List<SelectExpression> finalSelects = internalSchema
        .updateFinalSelectExpressions(finalSelectExpressions);

    return aggregated.select(
        finalSelects,
        contextStacker.push(PROJECT_OP_NAME),
        builder
    );
  }

  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  private static class InternalSchema {

    private final Optional<ColumnName> singleKeyColumn;
    private final List<SelectExpression> aggArgExpansions = new ArrayList<>();
    private final Map<String, ColumnName> expressionToInternalColumnName = new HashMap<>();

    InternalSchema(
        final LogicalSchema schema,
        final List<ColumnReferenceExp> requiredColumns,
        final List<Expression> aggregateFunctionArguments
    ) {
      this.singleKeyColumn = schema.key().size() == 1
          ? Optional.of(schema.key().get(0).name())
          : Optional.empty();

      final Set<String> seen = new HashSet<>();
      collectAggregateArgExpressions(requiredColumns, seen);
      collectAggregateArgExpressions(aggregateFunctionArguments, seen);
    }

    private void collectAggregateArgExpressions(
        final Collection<? extends Expression> expressions,
        final Set<String> seen
    ) {
      for (final Expression expression : expressions) {
        if (seen.contains(expression.toString())) {
          continue;
        }

        seen.add(expression.toString());

        final String internalName = INTERNAL_COLUMN_NAME_PREFIX + aggArgExpansions.size();

        aggArgExpansions.add(SelectExpression.of(ColumnName.of(internalName), expression));
        expressionToInternalColumnName
            .putIfAbsent(expression.toString(), ColumnName.of(internalName));
      }
    }

    List<Expression> resolveGroupByExpressions(
        final List<Expression> expressionList,
        final SchemaKStream<?> aggregateArgExpanded
    ) {
      final boolean specialRowTimeHandling = !(aggregateArgExpanded instanceof SchemaKTable);

      final Function<Expression, Expression> mapper = e -> {
        final boolean rowKey = singleKeyColumn.isPresent()
            && e instanceof UnqualifiedColumnReferenceExp
            && ((UnqualifiedColumnReferenceExp) e).getColumnName().equals(singleKeyColumn.get());

        if (!rowKey || !specialRowTimeHandling) {
          return resolveToInternal(e);
        }

        return e;
      };

      return expressionList.stream()
          .map(mapper)
          .collect(Collectors.toList());
    }

    /**
     * Return the aggregate function arguments based on the internal expressions.
     * Currently we support aggregate functions with at most two arguments where
     * the second argument should be a literal.
     * @param argExpressionList The list of parameters for the aggregate fuunction.
     * @return The list of arguments based on the internal expressions for the aggregate function.
     */
    List<Expression> getInternalArgsExpressionList(final List<Expression> argExpressionList) {
      // Currently we only support aggregations on one column only
      if (argExpressionList.size() > 2) {
        throw new KsqlException("Currently, KSQL UDAFs can only have two arguments.");
      }
      if (argExpressionList.isEmpty()) {
        return Collections.emptyList();
      }
      final List<Expression> internalExpressionList = new ArrayList<>();
      internalExpressionList.add(resolveToInternal(argExpressionList.get(0)));
      if (argExpressionList.size() == 2) {
        if (! (argExpressionList.get(1) instanceof Literal)) {
          throw new KsqlException("Currently, second argument in UDAF should be literal.");
        }
        internalExpressionList.add(argExpressionList.get(1));
      }
      return internalExpressionList;

    }

    List<SelectExpression> updateFinalSelectExpressions(
        final List<SelectExpression> finalSelectExpressions
    ) {
      return finalSelectExpressions.stream()
          .map(finalSelectExpression -> {
            final Expression internal = resolveToInternal(finalSelectExpression.getExpression());
            return SelectExpression.of(finalSelectExpression.getAlias(), internal);
          })
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
      public Optional<Expression> visitColumnReference(
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

        final boolean isAggregate = node.getColumnName().isAggregate();
        final boolean windowBounds = SchemaUtil.isWindowBound(node.getColumnName());

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
