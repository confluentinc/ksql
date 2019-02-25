/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionRewriter;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.structured.QueryContext;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryLoggerUtil;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;


public class AggregateNode extends PlanNode {

  private static final String INTERNAL_COLUMN_NAME_PREFIX = "KSQL_INTERNAL_COL_";

  private static final String PREPARE_OP_NAME = "prepare";
  private static final String AGGREGATION_OP_NAME = "aggregate";
  private static final String GROUP_BY_OP_NAME = "groupby";
  private static final String FILTER_OP_NAME = "filter";
  private static final String PROJECT_OP_NAME = "project";

  private final PlanNode source;
  private final Schema schema;
  private final List<Expression> groupByExpressions;
  private final WindowExpression windowExpression;
  private final List<Expression> aggregateFunctionArguments;
  private final List<FunctionCall> functionList;
  private final Set<DereferenceExpression> requiredColumns;
  private final List<Expression> finalSelectExpressions;
  private final Expression havingExpressions;

  @JsonCreator
  public AggregateNode(
      @JsonProperty("id") final PlanNodeId id,
      @JsonProperty("source") final PlanNode source,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("groupby") final List<Expression> groupByExpressions,
      @JsonProperty("window") final WindowExpression windowExpression,
      @JsonProperty("aggregateFunctionArguments") final List<Expression> aggregateFunctionArguments,
      @JsonProperty("functionList") final List<FunctionCall> functionList,
      @JsonProperty("requiredColumnList") final Set<DereferenceExpression> requiredColumns,
      @JsonProperty("finalSelectExpressions") final List<Expression> finalSelectExpressions,
      @JsonProperty("havingExpressions") final Expression havingExpressions
  ) {
    super(id, DataSourceType.KTABLE);

    this.source = source;
    this.schema = schema;
    this.groupByExpressions = groupByExpressions;
    this.windowExpression = windowExpression;
    this.aggregateFunctionArguments = aggregateFunctionArguments;
    this.functionList = functionList;
    this.requiredColumns =
        ImmutableSet.copyOf(Objects.requireNonNull(requiredColumns, "requiredColumns"));
    this.finalSelectExpressions = finalSelectExpressions;
    this.havingExpressions = havingExpressions;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public Field getKeyField() {
    return null;
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

  public WindowExpression getWindowExpression() {
    return windowExpression;
  }

  public List<Expression> getAggregateFunctionArguments() {
    return aggregateFunctionArguments;
  }

  public List<FunctionCall> getFunctionList() {
    return functionList;
  }

  public Set<DereferenceExpression> getRequiredColumnList() {
    return requiredColumns;
  }

  private List<SelectExpression> getFinalSelectExpressions() {
    final List<SelectExpression> finalSelectExpressionList = new ArrayList<>();
    if (finalSelectExpressions.size() != schema.fields().size()) {
      throw new RuntimeException(
          "Incompatible aggregate schema, field count must match, "
              + "selected field count:"
              + finalSelectExpressions.size()
              + " schema field count:"
              + schema.fields().size());
    }
    for (int i = 0; i < finalSelectExpressions.size(); i++) {
      finalSelectExpressionList.add(SelectExpression.of(
          schema.fields().get(i).name(),
          finalSelectExpressions.get(i)
      ));
    }
    return finalSelectExpressionList;
  }

  private Expression getHavingExpressions() {
    return havingExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitAggregate(this, context);
  }

  @SuppressWarnings("unchecked") // needs investigating
  @Override
  public SchemaKStream<?> buildStream(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final ServiceContext serviceContext,
      final ProcessingLogContext processingLogContext,
      final FunctionRegistry functionRegistry,
      final QueryId queryId
  ) {
    final QueryContext.Stacker contextStacker = buildNodeContext(queryId);
    final StructuredDataSourceNode streamSourceNode = getTheSourceNode();
    final SchemaKStream sourceSchemaKStream = getSource().buildStream(
        builder,
        ksqlConfig,
        serviceContext,
        processingLogContext,
        functionRegistry,
        queryId
    );

    // Pre aggregate computations
    final InternalSchema internalSchema = new InternalSchema(getRequiredColumnList(),
        getAggregateFunctionArguments());

    final SchemaKStream aggregateArgExpanded =
        sourceSchemaKStream.select(
            internalSchema.getAggArgExpansionList(),
            contextStacker.push(PREPARE_OP_NAME),
            processingLogContext);

    final QueryContext.Stacker groupByContext = contextStacker.push(GROUP_BY_OP_NAME);

    final KsqlTopicSerDe ksqlTopicSerDe = streamSourceNode.getStructuredDataSource()
        .getKsqlTopicSerde();
    final Serde<GenericRow> genericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggregateArgExpanded.getSchema(),
        ksqlConfig,
        true,
        serviceContext.getSchemaRegistryClientFactory(),
        QueryLoggerUtil.queryLoggerName(groupByContext.getQueryContext()),
        processingLogContext
    );

    final List<Expression> internalGroupByColumns = internalSchema.getInternalExpressionList(
        getGroupByExpressions());

    final SchemaKGroupedStream schemaKGroupedStream =
        aggregateArgExpanded.groupBy(
            genericRowSerde, internalGroupByColumns,
            groupByContext);

    // Aggregate computations
    final SchemaBuilder aggregateSchema = SchemaBuilder.struct();
    final Map<Integer, Integer> aggValToValColumnMap = createAggregateValueToValueColumnMap(
        aggregateArgExpanded,
        aggregateSchema,
        internalSchema
    );

    final Schema aggStageSchema = buildAggregateSchema(
        aggregateArgExpanded.getSchema(),
        functionRegistry,
        internalSchema
    );

    final QueryContext.Stacker aggregationContext = contextStacker.push(AGGREGATION_OP_NAME);

    final Serde<GenericRow> aggValueGenericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggStageSchema,
        ksqlConfig,
        true,
        serviceContext.getSchemaRegistryClientFactory(),
        QueryLoggerUtil.queryLoggerName(aggregationContext.getQueryContext()),
        processingLogContext
    );

    final KudafInitializer initializer = new KudafInitializer(aggValToValColumnMap.size());

    final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap = createAggValToFunctionMap(
        aggregateArgExpanded, aggregateSchema, initializer, aggValToValColumnMap.size(),
        functionRegistry, internalSchema);

    final SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        initializer,
        aggValToFunctionMap,
        aggValToValColumnMap,
        getWindowExpression(),
        aggValueGenericRowSerde,
        aggregationContext);

    SchemaKTable<?> result = new SchemaKTable<>(
        aggStageSchema,
        schemaKTable.getKtable(),
        schemaKTable.getKeyField(),
        schemaKTable.getSourceSchemaKStreams(),
        schemaKTable.getKeySerde(),
        SchemaKStream.Type.AGGREGATE,
        ksqlConfig,
        functionRegistry,
        aggregationContext.getQueryContext()
    );

    if (havingExpressions != null) {
      result = result.filter(
          internalSchema.resolveToInternal(havingExpressions),
          contextStacker.push(FILTER_OP_NAME),
          processingLogContext);
    }

    return result.select(
        internalSchema.updateFinalSelectExpressions(getFinalSelectExpressions()),
        contextStacker.push(PROJECT_OP_NAME),
        processingLogContext);
  }

  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  private Map<Integer, Integer> createAggregateValueToValueColumnMap(
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema,
      final InternalSchema internalSchema
  ) {
    final Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    int nonAggColumnIndex = 0;
    for (final Expression expression : getRequiredColumnList()) {
      final String exprStr =
          internalSchema.getInternalColumnForExpression(expression);
      final int index = SchemaUtil.getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
      aggValToValColumnMap.put(nonAggColumnIndex, index);
      nonAggColumnIndex++;
      final Field field = aggregateArgExpanded.getSchema().fields().get(index);
      aggregateSchema.field(field.name(), field.schema());
    }
    return aggValToValColumnMap;
  }


  private Map<Integer, KsqlAggregateFunction> createAggValToFunctionMap(
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema,
      final KudafInitializer initializer,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final InternalSchema internalSchema
  ) {
    try {
      int udafIndexInAggSchema = initialUdafIndex;
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
      for (final FunctionCall functionCall : getFunctionList()) {
        final KsqlAggregateFunction aggregateFunction = getAggregateFunction(
            functionRegistry,
            internalSchema,
            functionCall, aggregateArgExpanded.getSchema());

        aggValToAggFunctionMap.put(udafIndexInAggSchema++, aggregateFunction);
        initializer.addAggregateIntializer(aggregateFunction.getInitialValueSupplier());

        aggregateSchema.field("AGG_COL_"
            + udafIndexInAggSchema, aggregateFunction.getReturnType());
      }
      return aggValToAggFunctionMap;
    } catch (final Exception e) {
      throw new KsqlException(
          String.format(
              "Failed to create aggregate val to function map. expressionNames:%s",
              internalSchema.internalNameToIndexMap.keySet()
          ),
          e
      );
    }
  }

  private KsqlAggregateFunction getAggregateFunction(final FunctionRegistry functionRegistry,
      final InternalSchema internalSchema,
      final FunctionCall functionCall,
      final Schema schema) {
    final ExpressionTypeManager expressionTypeManager =
        new ExpressionTypeManager(schema, functionRegistry);
    final List<Expression> functionArgs = internalSchema.getInternalArgsExpressionList(
        functionCall.getArguments());
    final Schema expressionType = expressionTypeManager.getExpressionSchema(functionArgs.get(0));
    final KsqlAggregateFunction aggregateFunctionInfo = functionRegistry
        .getAggregate(functionCall.getName().toString(), expressionType);

    final List<String> args = functionArgs.stream()
        .map(Expression::toString)
        .collect(Collectors.toList());

    final int udafIndex = internalSchema.internalNameToIndexMap.get(args.get(0));

    return aggregateFunctionInfo.getInstance(new AggregateFunctionArguments(udafIndex, args));
  }

  private Schema buildAggregateSchema(
      final Schema schema,
      final FunctionRegistry functionRegistry,
      final InternalSchema internalSchema
  ) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final List<Field> fields = schema.fields();
    for (int i = 0; i < getRequiredColumnList().size(); i++) {
      schemaBuilder.field(fields.get(i).name(), fields.get(i).schema());
    }
    for (int aggFunctionVarSuffix = 0;
        aggFunctionVarSuffix < getFunctionList().size(); aggFunctionVarSuffix++) {
      final KsqlAggregateFunction aggregateFunction = getAggregateFunction(
          functionRegistry,
          internalSchema,
          getFunctionList().get(aggFunctionVarSuffix),
          schema);
      schemaBuilder.field(
          AggregateExpressionRewriter.AGGREGATE_FUNCTION_VARIABLE_PREFIX
              + aggFunctionVarSuffix,
          aggregateFunction.getReturnType()
      );
    }

    return schemaBuilder.build();
  }

  private static class InternalSchema {
    private final List<SelectExpression> aggArgExpansionList = new ArrayList<>();
    private final Map<String, Integer> internalNameToIndexMap = new HashMap<>();
    private final Map<String, String> expressionToInternalColumnNameMap = new HashMap<>();

    InternalSchema(
        final Set<DereferenceExpression> requiredColumns,
        final List<Expression> aggregateFunctionArguments) {
      final Set<String> seen = new HashSet<>();
      collectAggregateArgExpressions(requiredColumns, seen);
      collectAggregateArgExpressions(aggregateFunctionArguments, seen);
    }

    private void collectAggregateArgExpressions(
        final Collection<? extends Expression> expressions,
        final Set<String> seen
    ) {
      expressions.stream()
          .filter(e -> !seen.contains(e.toString()))
          .forEach(expression -> {
            final String internalColumnName = INTERNAL_COLUMN_NAME_PREFIX
                + aggArgExpansionList.size();
            seen.add(expression.toString());
            internalNameToIndexMap.put(internalColumnName, aggArgExpansionList.size());
            aggArgExpansionList.add(SelectExpression.of(internalColumnName, expression));
            expressionToInternalColumnNameMap
                .putIfAbsent(expression.toString(), internalColumnName);
          });
    }

    List<Expression> getInternalExpressionList(final List<Expression> expressionList) {
      return expressionList.stream()
          .map(this::resolveToInternal)
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
            return SelectExpression.of(finalSelectExpression.getName(), internal);
          })
          .collect(Collectors.toList());
    }

    String getInternalColumnForExpression(final Expression expression) {
      return expressionToInternalColumnNameMap.get(expression.toString());
    }

    List<SelectExpression> getAggArgExpansionList() {
      return aggArgExpansionList;
    }


    private Expression resolveToInternal(final Expression exp) {
      final String name = expressionToInternalColumnNameMap.get(exp.toString());
      if (name != null) {
        return new QualifiedNameReference(QualifiedName.of(name));
      }

      return ExpressionTreeRewriter.rewriteWith(new ResolveToInternalRewriter(), exp);
    }

    private class ResolveToInternalRewriter extends ExpressionRewriter<Void> {

      @Override
      public Expression rewriteDereferenceExpression(
          final DereferenceExpression node,
          final Void context,
          final ExpressionTreeRewriter<Void> treeRewriter
      ) {
        final String name = expressionToInternalColumnNameMap.get(node.toString());
        if (name != null) {
          return new QualifiedNameReference(QualifiedName.of(name));
        }

        throw new KsqlException("Unknown source column: " + node.toString());
      }
    }
  }
}
