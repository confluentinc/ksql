/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.AggregateFunctionArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SelectExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;


public class AggregateNode extends PlanNode {

  private static final String INTERNAL_COLUMN_NAME_PREFIX = "KSQL_INTERNAL_COL_";

  private final PlanNode source;
  private final Schema schema;
  private final List<Expression> groupByExpressions;
  private final WindowExpression windowExpression;
  private final List<Expression> aggregateFunctionArguments;
  private final List<FunctionCall> functionList;
  private final List<Expression> requiredColumnList;
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
      @JsonProperty("requiredColumnList") final List<Expression> requiredColumnList,
      @JsonProperty("finalSelectExpressions") final List<Expression> finalSelectExpressions,
      @JsonProperty("havingExpressions") final Expression havingExpressions
  ) {
    super(id);

    this.source = source;
    this.schema = schema;
    this.groupByExpressions = groupByExpressions;
    this.windowExpression = windowExpression;
    this.aggregateFunctionArguments = aggregateFunctionArguments;
    this.functionList = functionList;
    this.requiredColumnList = requiredColumnList;
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

  public List<Expression> getRequiredColumnList() {
    return requiredColumnList;
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

  public Expression getHavingExpressions() {
    return havingExpressions;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitAggregate(this, context);
  }

  @SuppressWarnings("unchecked") // needs investigating
  @Override
  public SchemaKStream buildStream(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final StructuredDataSourceNode streamSourceNode = getTheSourceNode();
    final SchemaKStream sourceSchemaKStream = getSource().buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props,
        schemaRegistryClientFactory
    );

    // Pre aggregate computations
    final InternalSchema internalSchema = new InternalSchema(getRequiredColumnList(),
        getAggregateFunctionArguments());

    final SchemaKStream aggregateArgExpanded =
        sourceSchemaKStream.select(internalSchema.getAggArgExpansionList());

    final KsqlTopicSerDe ksqlTopicSerDe = streamSourceNode.getStructuredDataSource()
        .getKsqlTopic()
        .getKsqlTopicSerDe();
    final Serde<GenericRow> genericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggregateArgExpanded.getSchema(),
        ksqlConfig,
        true,
        schemaRegistryClientFactory
    );

    final List<Expression> internalGroupByColumns = internalSchema.getInternalExpressionList(
        getGroupByExpressions());

    final SchemaKGroupedStream schemaKGroupedStream =
        aggregateArgExpanded.groupBy(Serdes.String(), genericRowSerde, internalGroupByColumns);

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

    final Serde<GenericRow> aggValueGenericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggStageSchema,
        ksqlConfig,
        true,
        schemaRegistryClientFactory
    );

    final KudafInitializer initializer = new KudafInitializer(aggValToValColumnMap.size());

    final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap = createAggValToFunctionMap(
        aggregateArgExpanded, aggregateSchema, initializer, aggValToValColumnMap.size(),
        functionRegistry, internalSchema);

    final SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        initializer, aggValToFunctionMap, aggValToValColumnMap, getWindowExpression(),
        aggValueGenericRowSerde);

    SchemaKTable result = new SchemaKTable(
        aggStageSchema,
        schemaKTable.getKtable(),
        schemaKTable.getKeyField(),
        schemaKTable.getSourceSchemaKStreams(),
        schemaKTable.isWindowed(),
        SchemaKStream.Type.AGGREGATE,
        functionRegistry,
        schemaRegistryClientFactory.get()
    );

    if (getHavingExpressions() != null) {
      result = result.filter(getHavingExpressions());
    }

    return result.select(internalSchema.updateFinalSelectExpressions(getFinalSelectExpressions()));
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
              internalSchema.getInternalNameToIndexMap()
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

    return aggregateFunctionInfo.getInstance(
        new AggregateFunctionArguments(internalSchema.getInternalNameToIndexMap(),
            functionArgs.stream().map(Expression::toString).collect(Collectors.toList())));
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
        final List<Expression> requiredColumnList,
        final List<Expression> aggregateFunctionArguments) {
      collectAggregateArgExpressions(requiredColumnList);
      collectAggregateArgExpressions(aggregateFunctionArguments);
    }


    private void collectAggregateArgExpressions(
        final List<Expression> expressions
    ) {
      expressions.stream()
          .filter(e -> !internalNameToIndexMap.containsKey(e.toString()))
          .forEach(expression -> {
            final String internalColumnName = INTERNAL_COLUMN_NAME_PREFIX
                + aggArgExpansionList.size();
            internalNameToIndexMap.put(internalColumnName, aggArgExpansionList.size());
            aggArgExpansionList.add(SelectExpression.of(internalColumnName, expression));
            expressionToInternalColumnNameMap
                .putIfAbsent(expression.toString(), internalColumnName);
          });
    }

    List<Expression> getInternalExpressionList(final List<Expression> expressionList) {
      return expressionList.stream()
          .map(argExpression -> new QualifiedNameReference(
              QualifiedName.of(getExpressionToInternalColumnNameMap()
                  .get(argExpression.toString()))))
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
      final List<Expression> internalExpressionList = new ArrayList<>();
      if (argExpressionList.isEmpty()) {
        return Collections.emptyList();
      }
      internalExpressionList.add(new QualifiedNameReference(
          QualifiedName.of(getExpressionToInternalColumnNameMap()
              .get(argExpressionList.get(0).toString())
          )));
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
            final String internal = expressionToInternalColumnNameMap
                .get(finalSelectExpression.getExpression().toString());

            if (internal == null) {
              return finalSelectExpression;
            }

            return SelectExpression.of(
                finalSelectExpression.getName(),
                new QualifiedNameReference(QualifiedName.of(internal)));

          })
          .collect(Collectors.toList());
    }

    String getInternalColumnForExpression(final Expression expression) {
      return expressionToInternalColumnNameMap.get(expression.toString());
    }

    List<SelectExpression> getAggArgExpansionList() {
      return aggArgExpansionList;
    }

    Map<String, Integer> getInternalNameToIndexMap() {
      return internalNameToIndexMap;
    }

    private Map<String, String> getExpressionToInternalColumnNameMap() {
      return expressionToInternalColumnNameMap;
    }
  }

}