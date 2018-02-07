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

import io.confluent.ksql.parser.tree.DereferenceExpression;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;


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
      final MetastoreUtil metastoreUtil,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    final StructuredDataSourceNode streamSourceNode = getTheSourceNode();
    final SchemaKStream sourceSchemaKStream = getSource().buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        metastoreUtil,
        functionRegistry,
        props,
        schemaRegistryClient
    );
    final SchemaKStream rekeyedSchemaKStream = rekeyRequired(sourceSchemaKStream) ?
        aggregateReKey(
            sourceSchemaKStream,
            functionRegistry,
            schemaRegistryClient) :
        sourceSchemaKStream;

    // Pre aggregate computations
    final List<Pair<String, Expression>> aggArgExpansionList = new ArrayList<>();
    final Map<String, Integer> expressionNames = new HashMap<>();
    collectAggregateArgExpressions(getRequiredColumnList(), aggArgExpansionList, expressionNames);
    collectAggregateArgExpressions(
        getAggregateFunctionArguments(),
        aggArgExpansionList,
        expressionNames
    );

    final SchemaKStream aggregateArgExpanded = rekeyedSchemaKStream.select(aggArgExpansionList);

    KsqlTopicSerDe ksqlTopicSerDe = streamSourceNode.getStructuredDataSource()
        .getKsqlTopic()
        .getKsqlTopicSerDe();
    final Serde<GenericRow> genericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggregateArgExpanded.getSchema(),
        ksqlConfig,
        true,
        schemaRegistryClient
    );

    final SchemaKGroupedStream schemaKGroupedStream =
        aggregateArgExpanded.groupByKey(Serdes.String(), genericRowSerde);

    // Aggregate computations
    final SchemaBuilder aggregateSchema = SchemaBuilder.struct();
    final Map<Integer, Integer> aggValToValColumnMap = createAggregateValueToValueColumnMap(
        aggregateArgExpanded,
        aggregateSchema
    );

    final List<Object> resultColumns = IntStream.range(
        0,
        aggValToValColumnMap.size()
    ).mapToObj(value -> "").collect(Collectors.toList());

    final Schema aggStageSchema = buildAggregateSchema(
        aggregateArgExpanded.getSchema(),
        functionRegistry
    );

    final Serde<GenericRow> aggValueGenericRowSerde = ksqlTopicSerDe.getGenericRowSerde(
        aggStageSchema,
        ksqlConfig,
        true,
        schemaRegistryClient
    );

    final SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        new KudafInitializer(resultColumns),
        new KudafAggregator(
            createAggValToFunctionMap(
                expressionNames,
                aggregateArgExpanded,
                aggregateSchema,
                resultColumns,
                functionRegistry
            ),
            aggValToValColumnMap
        ), getWindowExpression(),
        aggValueGenericRowSerde, "KSQL_Agg_Query_" + System.currentTimeMillis()
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

    return result.select(getFinalSelectExpressions());
  }

  private String fieldNameFromExpression(Expression expression) {
    if (expression instanceof DereferenceExpression) {
      DereferenceExpression dereferenceExpression =
          (DereferenceExpression) expression;
      return dereferenceExpression.getFieldName();
    }
    return null;
  }

  private boolean rekeyRequired(final SchemaKStream sourceSchemaKStream) {
    Field keyField = sourceSchemaKStream.getKeyField();
    if (keyField == null) {
      return true;
    }
    String keyFieldName = SchemaUtil.getFieldNameWithNoAlias(keyField);
    List<Expression> groupBy = getGroupByExpressions();
    return !(groupBy.size() == 1 && fieldNameFromExpression(groupBy.get(0)).equals(keyFieldName));
  }

  private SchemaKStream aggregateReKey(
      final SchemaKStream sourceSchemaKStream,
      final FunctionRegistry functionRegistry,
      SchemaRegistryClient schemaRegistryClient
  ) {
    StringBuilder aggregateKeyName = new StringBuilder();
    List<Integer> newKeyIndexes = new ArrayList<>();
    boolean addSeparator = false;
    for (Expression groupByExpr : getGroupByExpressions()) {
      if (addSeparator) {
        aggregateKeyName.append("|+|");
      } else {
        addSeparator = true;
      }
      aggregateKeyName.append(groupByExpr.toString());
      newKeyIndexes.add(getIndexInSchema(groupByExpr.toString(), sourceSchemaKStream.getSchema()));
    }

    KStream
        rekeyedKStream =
        sourceSchemaKStream
            .getKstream()
            .selectKey((KeyValueMapper<String, GenericRow, String>) (key, value) -> {
              StringBuilder newKey = new StringBuilder();
              boolean addSeparator1 = false;
              for (int index : newKeyIndexes) {
                if (addSeparator1) {
                  newKey.append("|+|");
                } else {
                  addSeparator1 = true;
                }
                newKey.append(String.valueOf(value.getColumns().get(index)));
              }
              return newKey.toString();
            });

    Field newKeyField = new Field(aggregateKeyName.toString(), -1, Schema.STRING_SCHEMA);

    return new SchemaKStream(
        sourceSchemaKStream.getSchema(),
        rekeyedKStream,
        newKeyField,
        Collections.singletonList(sourceSchemaKStream),
        SchemaKStream.Type.REKEY,
        functionRegistry,
        schemaRegistryClient
    );
  }

  private Map<Integer, Integer> createAggregateValueToValueColumnMap(
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema
  ) {
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    int nonAggColumnIndex = 0;
    for (Expression expression : getRequiredColumnList()) {
      String exprStr = expression.toString();
      int index = getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
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
      final Map<String, Integer> expressionNames
  ) {
    expressions.stream()
        .filter(e -> !expressionNames.containsKey(e.toString()))
        .forEach(expression -> {
          expressionNames.put(expression.toString(), aggArgExpansionList.size());
          aggArgExpansionList.add(new Pair<>(expression.toString(), expression));
        });
  }

  private int getIndexInSchema(final String fieldName, final Schema schema) {
    List<Field> fields = schema.fields();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (field.name().equals(fieldName)) {
        return i;
      }
    }
    throw new KsqlException(
        "Couldn't find field with name="
        + fieldName
        + " in schema. fields="
        + fields
    );
  }

  private Map<Integer, KsqlAggregateFunction> createAggValToFunctionMap(
      final Map<String, Integer> expressionNames,
      final SchemaKStream aggregateArgExpanded,
      final SchemaBuilder aggregateSchema,
      final List<Object> resultColumns,
      final FunctionRegistry functionRegistry
  ) {
    try {
      int udafIndexInAggSchema = resultColumns.size();
      final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
      for (FunctionCall functionCall : getFunctionList()) {
        KsqlAggregateFunction aggregateFunctionInfo = functionRegistry
            .getAggregateFunction(functionCall
                                      .getName()
                                      .toString(),
                                  functionCall
                                      .getArguments(), aggregateArgExpanded.getSchema()
            );
        KsqlAggregateFunction aggregateFunction = aggregateFunctionInfo.getInstance(
            expressionNames,
            functionCall.getArguments()
        );

        aggValToAggFunctionMap.put(udafIndexInAggSchema++, aggregateFunction);
        resultColumns.add(aggregateFunction.getIntialValue());

        aggregateSchema.field("AGG_COL_"
                              + udafIndexInAggSchema, aggregateFunction.getReturnType());
      }
      return aggValToAggFunctionMap;
    } catch (final Exception e) {
      throw new KsqlException(
          String.format(
              "Failed to create aggregate val to function map. expressionNames:%s, "
              + "resultColumns:%s",
              expressionNames,
              resultColumns
          ),
          e
      );
    }
  }

  private Schema buildAggregateSchema(
      final Schema schema,
      final FunctionRegistry functionRegistry
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
          getFunctionList().get(aggFunctionVarSuffix).getArguments(),
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

}
