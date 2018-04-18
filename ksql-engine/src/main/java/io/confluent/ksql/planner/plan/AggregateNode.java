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

  static final String INTERNAL_COLUMN_NAME_PREFIX = "KSQL_INTERNAL_COL_";

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
    InternalSchema internalSchema = new InternalSchema(getRequiredColumnList(),
                                                       getAggregateFunctionArguments());

    final SchemaKStream aggregateArgExpanded = maybeSetTheKeyField(
        sourceSchemaKStream.select(internalSchema.getAggArgExpansionList()),
        internalSchema);

    KsqlTopicSerDe ksqlTopicSerDe = streamSourceNode.getStructuredDataSource()
        .getKsqlTopic()
        .getKsqlTopicSerDe();
    final Serde<GenericRow> genericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggregateArgExpanded.getSchema(),
        ksqlConfig,
        true,
        schemaRegistryClient
    );

    List<Expression> internalGroupByColumns = internalSchema.getInternalExpressionList(
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
        schemaRegistryClient
    );

    final KudafInitializer initializer = new KudafInitializer(aggValToValColumnMap.size());
    final SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        initializer,
        new KudafAggregator(
            createAggValToFunctionMap(
                aggregateArgExpanded,
                aggregateSchema,
                initializer,
                aggValToValColumnMap.size(),
                functionRegistry,
                internalSchema
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

    return result.select(internalSchema.updateFinalSelectExpressions(
        getFinalSelectExpressions()));
  }

  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  private Map<Integer, Integer> createAggregateValueToValueColumnMap(
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema,
      final InternalSchema internalSchema
  ) {
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    int nonAggColumnIndex = 0;
    for (Expression expression : getRequiredColumnList()) {
      String exprStr =
          internalSchema.getInternalColumnForExpression(expression);
      int index = SchemaUtil.getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
      aggValToValColumnMap.put(nonAggColumnIndex, index);
      nonAggColumnIndex++;
      Field field = aggregateArgExpanded.getSchema().fields().get(index);
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
      for (FunctionCall functionCall : getFunctionList()) {
        KsqlAggregateFunction aggregateFunctionInfo = functionRegistry
            .getAggregateFunction(functionCall.getName().toString(),
                                  internalSchema.getInternalExpressionList(
                                      functionCall.getArguments()),
                                  aggregateArgExpanded.getSchema()
            );
        KsqlAggregateFunction aggregateFunction = aggregateFunctionInfo.getInstance(
            internalSchema.getInternalNameToIndexMap(),
            internalSchema.getInternalExpressionList(functionCall.getArguments())
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
              internalSchema.getInternalNameToIndexMap()
          ),
          e
      );
    }
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
      String udafName = getFunctionList().get(aggFunctionVarSuffix).getName()
          .getSuffix();
      KsqlAggregateFunction aggregateFunction = functionRegistry.getAggregateFunction(
          udafName,
          internalSchema.getInternalExpressionList(
              getFunctionList().get(aggFunctionVarSuffix).getArguments()),
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

  private SchemaKStream maybeSetTheKeyField(SchemaKStream schemaKStream,
                                            InternalSchema internalSchema) {
    Map<String, String> fieldsWithNoAlias = internalSchema
        .getExpressionToInternalColumnNameMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            e -> e.getKey().contains(".")
                 ? e.getKey().substring(e.getKey().indexOf(".") + 1)
                 : e.getKey(),
            e -> e.getValue()
        ));

    if (schemaKStream.getKeyField() != null) {
      String keyFieldName = schemaKStream.getKeyField().name();
      return new SchemaKStream(schemaKStream.getSchema(),
                               schemaKStream.getKstream(),
                               fieldsWithNoAlias
                                   .containsKey(keyFieldName)
                               ? new Field(fieldsWithNoAlias.get(keyFieldName),
                                           schemaKStream.getKeyField().index(),
                                           schemaKStream.getKeyField().schema()
                               )
                               : null,
                               schemaKStream.getSourceSchemaKStreams(),
                               schemaKStream.getType(),
                               schemaKStream.getFunctionRegistry(),
                               schemaKStream.getSchemaRegistryClient());
    }
    return schemaKStream;
  }


  class InternalSchema {
    private final List<Pair<String, Expression>> aggArgExpansionList = new ArrayList<>();
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
            String internalColumnName = INTERNAL_COLUMN_NAME_PREFIX + aggArgExpansionList.size();
            internalNameToIndexMap.put(internalColumnName, aggArgExpansionList.size());
            aggArgExpansionList.add(new Pair<>(internalColumnName, expression));
            if (!expressionToInternalColumnNameMap.containsKey(expression.toString())) {
              expressionToInternalColumnNameMap.put(expression.toString(), internalColumnName);
            }
          });
    }

    List<Expression> getInternalExpressionList(final List<Expression> expressionList) {
      return expressionList.stream()
          .map(argExpression -> argExpression instanceof Literal
                                ? argExpression
                                : new QualifiedNameReference(
                                    QualifiedName.of(getExpressionToInternalColumnNameMap()
                                            .get(argExpression.toString()))))
          .collect(Collectors.toList());
    }

    List<Pair<String, Expression>> updateFinalSelectExpressions(
        final List<Pair<String, Expression>> finalSelectExpressions) {
      return finalSelectExpressions.stream()
          .map(finalSelectExpression ->
                   expressionToInternalColumnNameMap
                       .containsKey(finalSelectExpression.getRight().toString())
                   ? new Pair<>(finalSelectExpression.getLeft(),
                                (Expression)
                                    new QualifiedNameReference(
                                        QualifiedName.of(
                                            expressionToInternalColumnNameMap
                                                .get(finalSelectExpression.getRight().toString()))))
                   : new Pair<>(finalSelectExpression.getLeft(), finalSelectExpression.getRight()))
          .collect(Collectors.toList());
    }

    String getInternalColumnForExpression(Expression expression) {
      return expressionToInternalColumnNameMap.get(expression.toString());
    }

    List<Pair<String, Expression>> getAggArgExpansionList() {
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
