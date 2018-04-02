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

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.KudafAggregator;
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
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;


public class AggregateNode extends PlanNode {

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

  private List<Pair<String, Expression>> getFinalSelectExpressions() {
    List<Pair<String, Expression>> finalSelectExpressionList = new ArrayList<>();
    if (finalSelectExpressions.size() != schema.fields().size()) {
      throw new KsqlException(
          "Incompatible aggregate schema, field count must match, "
          + "selected field count:"
          + finalSelectExpressions.size()
          + " schema field count:"
          + schema.fields().size());
    }
    for (int i = 0; i < finalSelectExpressions.size(); i++) {
      finalSelectExpressionList.add(new Pair<>(
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
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }

  @Override
  public SchemaKStream buildStream(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final StructuredDataSourceNode streamSourceNode = getTheSourceNode();
    final SchemaKStream sourceSchemaKStream = getSource().buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        props,
        schemaRegistryClient
    );

    if (sourceSchemaKStream instanceof SchemaKTable) {
      throw new KsqlException(
          "Unsupported aggregation. KSQL currently only supports aggregation on a Stream.");
    }

    // Pre aggregate computations
    final List<Pair<String, Expression>> aggArgExpansionList = new ArrayList<>();
    final Map<String, Integer> expressionNames = new HashMap<>();
    final Map<String, String> expressionToInternalColumnNameMap = new HashMap<>();
    collectAggregateArgExpressions(
        getRequiredColumnList(),
        aggArgExpansionList,
        expressionNames,
        expressionToInternalColumnNameMap
    );
    collectAggregateArgExpressions(
        getAggregateFunctionArguments(),
        aggArgExpansionList,
        expressionNames,
        expressionToInternalColumnNameMap
    );

    final SchemaKStream aggregateArgExpanded = sourceSchemaKStream.select(aggArgExpansionList);

    KsqlTopicSerDe ksqlTopicSerDe = streamSourceNode.getStructuredDataSource()
        .getKsqlTopic()
        .getKsqlTopicSerDe();
    final Serde<GenericRow> genericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggregateArgExpanded.getSchema(),
        ksqlConfig,
        true,
        schemaRegistryClient
    );

    List<Expression> internalGroupByColumns = getInternalExpressionList(
        getGroupByExpressions(),
        expressionToInternalColumnNameMap);

    final SchemaKGroupedStream schemaKGroupedStream =
        aggregateArgExpanded.groupBy(Serdes.String(), genericRowSerde, internalGroupByColumns);

    // Aggregate computations
    final SchemaBuilder aggregateSchema = SchemaBuilder.struct();
    final Map<Integer, Integer> aggValToValColumnMap = createAggregateValueToValueColumnMap(
        aggregateArgExpanded,
        aggregateSchema,
        expressionToInternalColumnNameMap
    );

    final Schema aggStageSchema = buildAggregateSchema(
        aggregateArgExpanded.getSchema(),
        functionRegistry,
        expressionToInternalColumnNameMap
    );

    final Serde<GenericRow> aggValueGenericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggStageSchema,
        ksqlConfig,
        true,
        schemaRegistryClient
    );

    final KudafInitializer initializer = new KudafInitializer(aggValToValColumnMap.size());
    final SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        initializer,
        new KudafAggregator(
            createAggValToFunctionMap(
                expressionNames,
                aggregateArgExpanded,
                aggregateSchema,
                initializer,
                aggValToValColumnMap.size(),
                functionRegistry,
                expressionToInternalColumnNameMap
            ),
            aggValToValColumnMap
        ), getWindowExpression(),
        aggValueGenericRowSerde
    );

    SchemaKTable result = new SchemaKTable(
        aggStageSchema,
        schemaKTable.getKtable(),
        schemaKTable.getKeyField(),
        schemaKTable.getSourceSchemaKStreams(),
        schemaKTable.isWindowed(),
        SchemaKStream.Type.AGGREGATE,
        functionRegistry,
        schemaRegistryClient
    );

    if (getHavingExpressions() != null) {
      result = result.filter(getHavingExpressions());
    }

    return result.select(updateFinalSelectExpressions(
        getFinalSelectExpressions(),
        expressionToInternalColumnNameMap));
  }

  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  private Map<Integer, Integer> createAggregateValueToValueColumnMap(
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema,
      final Map<String, String> expressionToInternalColumnNameMap
  ) {
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    int nonAggColumnIndex = 0;
    for (Expression expression : getRequiredColumnList()) {
      String exprStr = expressionToInternalColumnNameMap.get(expression.toString());
      int index = SchemaUtil.getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
      aggValToValColumnMap.put(nonAggColumnIndex, index);
      nonAggColumnIndex++;
      Field field = aggregateArgExpanded.getSchema().fields().get(index);
      aggregateSchema.field(field.name(), field.schema());
    }
    return aggValToValColumnMap;
  }

  private void collectAggregateArgExpressions(
      final List<Expression> expressions,
      final List<Pair<String, Expression>> aggArgExpansionList,
      final Map<String, Integer> expressionNames,
      final Map<String, String> expressionToInternalColumnNameMap
  ) {
    expressions.stream()
        .filter(e -> !expressionNames.containsKey(e.toString()))
        .forEach(expression -> {
          String internalColumnName = "KSQL_INTERNAL_COL_" + aggArgExpansionList.size();
          expressionNames.put(internalColumnName, aggArgExpansionList.size());
          aggArgExpansionList.add(new Pair<>(internalColumnName, expression));
          if (!expressionToInternalColumnNameMap.containsKey(expression.toString())) {
            expressionToInternalColumnNameMap.put(expression.toString(), internalColumnName);
          }
        });
  }

  private Map<Integer, KsqlAggregateFunction> createAggValToFunctionMap(
      final Map<String, Integer> expressionNames,
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema,
      final KudafInitializer initializer,
      final int initialUdafIndex,
      final FunctionRegistry functionRegistry,
      final Map<String, String> expressionToInternalColumnNameMap
  ) {
    try {
      int udafIndexInAggSchema = initialUdafIndex;
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
      for (FunctionCall functionCall : getFunctionList()) {
        KsqlAggregateFunction aggregateFunctionInfo = functionRegistry
            .getAggregateFunction(functionCall.getName().toString(),
                                  getInternalExpressionList(functionCall.getArguments(),
                                                            expressionToInternalColumnNameMap),
                                  aggregateArgExpanded.getSchema()
            );
        KsqlAggregateFunction aggregateFunction = aggregateFunctionInfo.getInstance(
            expressionNames,
            getInternalExpressionList(functionCall.getArguments(),
                                      expressionToInternalColumnNameMap)
        );

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
              expressionNames
          ),
          e
      );
    }
  }

  private Schema buildAggregateSchema(
      final Schema schema,
      final FunctionRegistry functionRegistry,
      final Map<String, String> expressionToInternalColumnNameMap
  ) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final List<Field> fields = schema.fields();
    for (int i = 0; i < getRequiredColumnList().size(); i++) {
      schemaBuilder.field(fields.get(i).name(), fields.get(i).schema());
    }
    for (int aggFunctionVarSuffix = 0;
         aggFunctionVarSuffix < getFunctionList().size(); aggFunctionVarSuffix++) {
      String udafName = getFunctionList().get(aggFunctionVarSuffix).getName()
          .getSuffix();
      KsqlAggregateFunction aggregateFunction = functionRegistry.getAggregateFunction(
          udafName,
          getInternalExpressionList(
              getFunctionList().get(aggFunctionVarSuffix).getArguments(),
              expressionToInternalColumnNameMap),
          schema
      );
      schemaBuilder.field(
          AggregateExpressionRewriter.AGGREGATE_FUNCTION_VARIABLE_PREFIX
          + aggFunctionVarSuffix,
          aggregateFunction.getReturnType()
      );
    }

    return schemaBuilder.build();
  }

  private List<Expression> getInternalExpressionList(
      final List<Expression> expressionList,
      final Map<String, String> expressionToInternalColumnNameMap
  ) {
    return expressionList.stream()
        .map(argExpression -> argExpression instanceof Literal
                              ? argExpression
                              : new QualifiedNameReference(
            QualifiedName.of(expressionToInternalColumnNameMap.get(argExpression.toString()))))
        .collect(Collectors.toList());
  }

  private List<Pair<String, Expression>> updateFinalSelectExpressions(
      final List<Pair<String, Expression>> finalSelectExpressions,
      final Map<String, String> expressionToInternalColumnNameMap) {
    return finalSelectExpressions.stream()
        .map(finalSelectExpression ->
                 expressionToInternalColumnNameMap
                     .containsKey(finalSelectExpression.getRight().toString())
                 ? new Pair<>(finalSelectExpression.getLeft(),
                            (Expression)
                            new QualifiedNameReference(
                                QualifiedName.of(expressionToInternalColumnNameMap
                                          .get(finalSelectExpression.getRight().toString()))))
                 : new Pair<>(finalSelectExpression.getLeft(), finalSelectExpression.getRight()))
        .collect(Collectors.toList());
  }

}
