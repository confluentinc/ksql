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

package io.confluent.ksql.physical;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunctionRegistry;
import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.util.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.KsqlBareOutputNode;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.SourceNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;
import io.confluent.ksql.util.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PhysicalPlanBuilder {

  private final StreamsBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final KafkaTopicClient kafkaTopicClient;
  private final MetastoreUtil metastoreUtil;
  private final KsqlFunctionRegistry ksqlFunctionRegistry;

  private static final KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>> nonWindowedMapper = (key, row) -> {
    if (row != null) {
      row.getColumns().add(0, key);

    }
    return new KeyValue<>(key, row);
  };

  private static final KeyValueMapper<Windowed<String>, GenericRow, KeyValue<Windowed<String>, GenericRow>> windowedMapper = (key, row) -> {
    if (row != null) {
      row.getColumns().add(0,
          String.format("%s : Window{start=%d end=-}", key
              .key(), key.window().start()));

    }
    return new KeyValue<>(key, row);
  };
  private final WindowedSerde windowedSerde = new WindowedSerde();

  private OutputNode planSink = null;

  public PhysicalPlanBuilder(final StreamsBuilder builder,
                             final KsqlConfig ksqlConfig,
                             final KafkaTopicClient kafkaTopicClient,
                             final MetastoreUtil metastoreUtil,
                             final KsqlFunctionRegistry ksqlFunctionRegistry) {
    this.builder = builder;
    this.ksqlConfig = ksqlConfig;
    this.kafkaTopicClient = kafkaTopicClient;
    this.metastoreUtil = metastoreUtil;
    this.ksqlFunctionRegistry = ksqlFunctionRegistry;
  }

  public SchemaKStream buildPhysicalPlan(final PlanNode logicalPlanRoot) throws Exception {
    return kafkaStreamsDsl(logicalPlanRoot);
  }

  private SchemaKStream kafkaStreamsDsl(final PlanNode planNode) throws Exception {
    return kafkaStreamsDsl(planNode, new HashMap<>());
  }

  private SchemaKStream kafkaStreamsDsl(final PlanNode planNode, Map<String, Object> propsMap) throws
                                                                                   Exception {
    if (planNode instanceof SourceNode) {
      return buildSource((SourceNode) planNode, propsMap);
    } else if (planNode instanceof JoinNode) {
      return buildJoin((JoinNode) planNode, propsMap);
    } else if (planNode instanceof AggregateNode) {
      return buildAggregate((AggregateNode) planNode);
    } else if (planNode instanceof ProjectNode) {
      return buildProject((ProjectNode) planNode);
    } else if (planNode instanceof FilterNode) {
      return buildFilter((FilterNode) planNode);
    } else if (planNode instanceof OutputNode) {
      return buildOutput((OutputNode) planNode);
    }
    throw new KsqlException(
        "Unsupported logical plan node: " + planNode.getId() + " , Type: " + planNode.getClass()
            .getName());
  }

  private SchemaKStream buildOutput(final OutputNode outputNode)
      throws Exception {
    final SchemaKStream schemaKStream = kafkaStreamsDsl(outputNode.getSource());
    final Set<Integer> rowkeyIndexes = SchemaUtil.getRowTimeRowKeyIndexes(outputNode.getSchema());

    if (outputNode instanceof KsqlStructuredDataOutputNode) {
      final KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode = (KsqlStructuredDataOutputNode)
          outputNode;

      final KsqlStructuredDataOutputNode.Builder outputNodeBuilder
          = KsqlStructuredDataOutputNode.Builder.from(ksqlStructuredDataOutputNode);
      final Schema schema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
          ksqlStructuredDataOutputNode.getSchema());
      outputNodeBuilder.withSchema(schema);

      if (ksqlStructuredDataOutputNode.getTopicSerde() instanceof KsqlAvroTopicSerDe) {
        addAvroSchemaToResultTopic(outputNodeBuilder, ksqlStructuredDataOutputNode, schema);
      }

      final Map<String, Object> outputProperties = ksqlStructuredDataOutputNode
          .getOutputProperties();

      if (outputProperties.containsKey(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY)) {
        ksqlConfig.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY,
                       outputProperties.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));
      }
      if (outputProperties.containsKey(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS_PROPERTY)) {
        ksqlConfig.put(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS_PROPERTY,
                       outputProperties.get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS_PROPERTY
                       ));
      }

      final SchemaKStream result = createOutputStream(schemaKStream,
          ksqlStructuredDataOutputNode,
          outputNodeBuilder,
          outputProperties);

      final KsqlStructuredDataOutputNode noRowKey = outputNodeBuilder.build();
      result.into(
          noRowKey.getKafkaTopicName(),
          SerDeUtil.getRowSerDe(
              noRowKey.getKsqlTopic().getKsqlTopicSerDe(),
              noRowKey.getSchema()),
          rowkeyIndexes,
          ksqlConfig,
          kafkaTopicClient);


      this.planSink = outputNodeBuilder.withSchema(SchemaUtil.addImplicitRowTimeRowKeyToSchema(noRowKey.getSchema()))
          .build();
      return result;
    } else if (outputNode instanceof KsqlBareOutputNode) {
      this.planSink = outputNode;
      return schemaKStream.toQueue(outputNode.getLimit());
    }

    throw new KsqlException("Unsupported output logical node: " + outputNode.getClass().getName());
  }

  private SchemaKStream createOutputStream(final SchemaKStream schemaKStream,
                                           final KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode,
                                           final KsqlStructuredDataOutputNode.Builder outputNodeBuilder,
                                           final Map<String, Object> outputProperties) {

    if (schemaKStream instanceof SchemaKTable) {
      return schemaKStream;
    }

    final SchemaKStream result = new SchemaKStream(ksqlStructuredDataOutputNode.getSchema(),
        schemaKStream.getKstream(),
        ksqlStructuredDataOutputNode
            .getKeyField(),
        Collections.singletonList(schemaKStream),
        SchemaKStream.Type.SINK, ksqlFunctionRegistry
    );

    if (outputProperties.containsKey(DdlConfig.PARTITION_BY_PROPERTY)) {
      String keyFieldName = outputProperties.get(DdlConfig.PARTITION_BY_PROPERTY).toString();
      Field keyField = SchemaUtil.getFieldByName(
          result.getSchema(), keyFieldName)
          .orElseThrow(() -> new KsqlException(String.format("Column %s does not exist in the result schema."
                  + " Error in Partition By clause.",
              keyFieldName)));

      outputNodeBuilder.withKeyField(keyField);
      return result.selectKey(keyField);
    }
    return result;
  }

  private SchemaKStream buildAggregate(final AggregateNode aggregateNode) throws Exception {

    final StructuredDataSourceNode streamSourceNode = aggregateNode.getTheSourceNode();
    final SchemaKStream sourceSchemaKStream = kafkaStreamsDsl(aggregateNode.getSource());
    final SchemaKStream rekeyedSchemaKStream = aggregateReKey(aggregateNode, sourceSchemaKStream);

    // Pre aggregate computations
    final List<Pair<String, Expression>> aggArgExpansionList = new ArrayList<>();
    final Map<String, Integer> expressionNames = new HashMap<>();
    collectAggregateArgExpressions(aggregateNode.getRequiredColumnList(), aggArgExpansionList, expressionNames);
    collectAggregateArgExpressions(aggregateNode.getAggregateFunctionArguments(), aggArgExpansionList, expressionNames);

    final SchemaKStream aggregateArgExpanded = rekeyedSchemaKStream.select(aggArgExpansionList);

    final Serde<GenericRow> genericRowSerde =
        SerDeUtil.getRowSerDe(streamSourceNode.getStructuredDataSource()
            .getKsqlTopic()
            .getKsqlTopicSerDe(),
        aggregateArgExpanded.getSchema());

    final SchemaKGroupedStream schemaKGroupedStream =
        aggregateArgExpanded.groupByKey(Serdes.String(), genericRowSerde);

    // Aggregate computations
    final SchemaBuilder aggregateSchema = SchemaBuilder.struct();

    final Map<Integer, Integer> aggValToValColumnMap = createAggregateValueToValueColumnMap(
        aggregateNode,
        aggregateArgExpanded,
        aggregateSchema);

    final List<Object> resultColumns = IntStream.range(0,
        aggValToValColumnMap.size()).mapToObj(value -> "").collect(Collectors.toList());

    final Serde<GenericRow> aggValueGenericRowSerde = SerDeUtil.getRowSerDe(streamSourceNode
            .getStructuredDataSource()
            .getKsqlTopic()
            .getKsqlTopicSerDe(),
        aggregateSchema);

    final SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        new KudafInitializer(resultColumns),
        new KudafAggregator(createAggValToFunctionMap(
            aggregateNode,
            expressionNames,
            aggregateArgExpanded,
            aggregateSchema,
            resultColumns),
            aggValToValColumnMap), aggregateNode.getWindowExpression(),
        aggValueGenericRowSerde, "KSQL_Agg_Query_" + System.currentTimeMillis());

    final Schema aggStageSchema = buildAggregateSchema(aggregateNode, schemaKTable);

    SchemaKTable result = new SchemaKTable(aggStageSchema, schemaKTable.getKtable(),
                                                      schemaKTable.getKeyField(),
                                                      schemaKTable.getSourceSchemaKStreams(),
                                                      schemaKTable.isWindowed(),
                                                      SchemaKStream.Type.AGGREGATE,
                                           ksqlFunctionRegistry);


    if (aggregateNode.getHavingExpressions() != null) {
      result = result.filter(aggregateNode.getHavingExpressions());
    }

    return result.select(aggregateNode.getFinalSelectExpressions());
  }

  private Schema buildAggregateSchema(AggregateNode aggregateNode, SchemaKTable schemaKTable) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final List<Field> fields = schemaKTable.getSchema().fields();
    for (int i = 0; i < aggregateNode.getRequiredColumnList().size(); i++) {
      schemaBuilder.field(fields.get(i).name(), fields.get(i).schema());
    }
    for (int aggFunctionVarSuffix = 0;
         aggFunctionVarSuffix < aggregateNode.getFunctionList().size(); aggFunctionVarSuffix++) {
      Schema fieldSchema;
      String udafName = aggregateNode.getFunctionList().get(aggFunctionVarSuffix).getName()
          .getSuffix();
      KsqlAggregateFunction aggregateFunction = ksqlFunctionRegistry.getAggregateFunction(udafName,
                                                                                          aggregateNode
              .getFunctionList()
              .get(aggFunctionVarSuffix).getArguments(), schemaKTable.getSchema());
      fieldSchema = aggregateFunction.getReturnType();
      schemaBuilder.field(AggregateExpressionRewriter.AGGREGATE_FUNCTION_VARIABLE_PREFIX
          + aggFunctionVarSuffix, fieldSchema);
    }

    return schemaBuilder.build();
  }

  private Map<Integer, KsqlAggregateFunction> createAggValToFunctionMap(final AggregateNode aggregateNode,
                                                                        final Map<String, Integer> expressionNames,
                                                                        final SchemaKStream aggregateArgExpanded,
                                                                        final SchemaBuilder aggregateSchema,
                                                                        final List<Object> resultColumns)
      throws InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, NoSuchMethodException {

    int udafIndexInAggSchema = resultColumns.size();
    final Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
    for (FunctionCall functionCall: aggregateNode.getFunctionList()) {
      KsqlAggregateFunction aggregateFunctionInfo = ksqlFunctionRegistry
          .getAggregateFunction(functionCall
              .getName()
              .toString(),
                                functionCall
              .getArguments(), aggregateArgExpanded.getSchema());
      int udafIndex = expressionNames.get(functionCall.getArguments().get(0).toString());
      KsqlAggregateFunction aggregateFunction = aggregateFunctionInfo.getClass()
          .getDeclaredConstructor(Integer.class).newInstance(udafIndex);
      aggValToAggFunctionMap.put(udafIndexInAggSchema++, aggregateFunction);
      resultColumns.add(aggregateFunction.getIntialValue());

      aggregateSchema.field("AGG_COL_"
                            + udafIndexInAggSchema, aggregateFunction.getReturnType());
    }
    return aggValToAggFunctionMap;
  }

  private Map<Integer, Integer> createAggregateValueToValueColumnMap(final AggregateNode aggregateNode,
                                                                     final SchemaKStream aggregateArgExpanded,
                                                                     final SchemaBuilder aggregateSchema) {
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    int nonAggColumnIndex = 0;
    for (Expression expression: aggregateNode.getRequiredColumnList()) {
      String exprStr = expression.toString();
      int index = getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
      aggValToValColumnMap.put(nonAggColumnIndex, index);
      nonAggColumnIndex++;
      Field field = aggregateArgExpanded.getSchema().fields().get(index);
      aggregateSchema.field(field.name(), field.schema());
    }
    return aggValToValColumnMap;
  }

  private void collectAggregateArgExpressions(final List<Expression> expressions,
                                              final List<Pair<String, Expression>> aggArgExpansionList,
                                              final Map<String, Integer> expressionNames) {
    expressions.stream()
        .filter(e -> !expressionNames.containsKey(e.toString()))
        .forEach(expression -> {
          expressionNames.put(expression.toString(), aggArgExpansionList.size());
          aggArgExpansionList.add(new Pair<>(expression.toString(), expression));
        });
  }

  private SchemaKStream buildProject(final ProjectNode projectNode)
      throws Exception {
    return kafkaStreamsDsl(projectNode.getSource()).select(projectNode.getProjectNameExpressionPairList());
  }


  private SchemaKStream buildFilter(final FilterNode filterNode)
      throws Exception {
    return kafkaStreamsDsl(filterNode.getSource()).filter(filterNode.getPredicate());
  }


  private SchemaKStream buildSource(final SourceNode sourceNode, Map<String, Object> props) {

    if (sourceNode instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) sourceNode;

      if (structuredDataSourceNode.getTimestampField() != null) {
        int timestampColumnIndex = getTimeStampColumnIndex(structuredDataSourceNode
                                                                      .getSchema(),
                                                                  structuredDataSourceNode
                                                                      .getTimestampField());
        ksqlConfig.put(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX, timestampColumnIndex);
      }

      Serde<GenericRow>
          genericRowSerde =
          SerDeUtil.getRowSerDe(structuredDataSourceNode.getStructuredDataSource()
                                    .getKsqlTopic().getKsqlTopicSerDe(),
                                SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
                                    structuredDataSourceNode.getSchema()));

      if (structuredDataSourceNode.getDataSourceType()
          == StructuredDataSource.DataSourceType.KTABLE) {
        final KsqlTable table = (KsqlTable) structuredDataSourceNode.getStructuredDataSource();

        final KTable kTable = createKTable(
            getAutoOffsetReset(props),
            table,
            genericRowSerde,
            SerDeUtil.getRowSerDe(table.getKsqlTopic().getKsqlTopicSerDe(),
                structuredDataSourceNode.getSchema())
        );
        return new SchemaKTable(sourceNode.getSchema(), kTable,
            sourceNode.getKeyField(), new ArrayList<>(),
            table.isWindowed(),
            SchemaKStream.Type.SOURCE, ksqlFunctionRegistry);
      }

      return new SchemaKStream(sourceNode.getSchema(),
          builder
              .stream(structuredDataSourceNode.getStructuredDataSource().getKsqlTopic().getKafkaTopicName(),
                  Consumed.with(Serdes.String(), genericRowSerde))
              .map(nonWindowedMapper)
              .transformValues(new AddTimestampColumnValueTransformerSupplier()),
          sourceNode.getKeyField(), new ArrayList<>(),
          SchemaKStream.Type.SOURCE, ksqlFunctionRegistry);
    }
    throw new KsqlException("Unsupported source logical node: " + sourceNode.getClass().getName());
  }

  private <K> KTable table(final KStream<K, GenericRow> stream, final Serde<K> keySerde, final Serde<GenericRow> valueSerde) {
    return stream.groupByKey(Serialized.with(keySerde, valueSerde))
        .reduce((genericRow, newValue) -> newValue);
  }

  @SuppressWarnings("unchecked")
  private KTable createKTable(final Topology.AutoOffsetReset autoOffsetReset,
                              final KsqlTable ksqlTable,
                              final Serde<GenericRow> genericRowSerde,
                              final Serde<GenericRow> genericRowSerdeAfterRead) {
    if (ksqlTable.isWindowed()) {
      return table(builder
          .stream(ksqlTable.getKsqlTopic().getKafkaTopicName(),
              Consumed.with(windowedSerde, genericRowSerde)
                  .withOffsetResetPolicy(autoOffsetReset))
          .map(windowedMapper)
          .transformValues(new AddTimestampColumnValueTransformerSupplier()), windowedSerde, genericRowSerdeAfterRead);
    } else {
      return table(builder
              .stream(ksqlTable.getKsqlTopic().getKafkaTopicName(),
                  Consumed.with(Serdes.String(), genericRowSerde)
                      .withOffsetResetPolicy(autoOffsetReset))
              .map(nonWindowedMapper)
              .transformValues(new AddTimestampColumnValueTransformerSupplier()),
          Serdes.String(), genericRowSerdeAfterRead);
    }
  }

  private Topology.AutoOffsetReset getAutoOffsetReset(Map<String, Object> props) {
    if (props.containsKey(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)) {
      if (props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG).toString()
          .equalsIgnoreCase("EARLIEST")) {
        return Topology.AutoOffsetReset.EARLIEST;
      } else if (props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG).toString()
          .equalsIgnoreCase("LATEST")) {
        return Topology.AutoOffsetReset.LATEST;
      }
    }
    return null;
  }

  private SchemaKStream buildJoin(final JoinNode joinNode, final Map<String, Object> propsMap)
      throws Exception {

    if (!joinNode.isLeftJoin()) {
      throw new KsqlException("Join type is not supported yet: " + joinNode.getType());
    }

    final SchemaKTable table = tableForJoin(joinNode, propsMap);
    final SchemaKStream stream = streamForJoin(kafkaStreamsDsl(joinNode.getLeft()), joinNode.getLeftKeyFieldName());

    final KsqlTopicSerDe joinSerDe = getResultTopicSerde(joinNode);
    return stream.leftJoin(table,
        joinNode.getSchema(),
        joinNode.getSchema().field(joinNode.getLeftAlias() + "." + stream.getKeyField().name()),
        joinSerDe);

  }

  private SchemaKTable tableForJoin(JoinNode joinNode, Map<String, Object> propsMap) throws Exception {
    final SchemaKStream rightStream = kafkaStreamsDsl(joinNode.getRight(), propsMap);

    if (!(rightStream instanceof SchemaKTable)) {
      throw new KsqlException("Unsupported join. Only stream-table joins are supported, but was "
          + joinNode.getLeft() + "-" + joinNode.getRight());
    }

    return (SchemaKTable) rightStream;
  }

  private SchemaKStream streamForJoin(final SchemaKStream stream, final String leftKeyFieldName) {
    if (stream.getKeyField() == null
        || !stream.getKeyField().name().equals(leftKeyFieldName)) {
      return
          stream.selectKey(SchemaUtil.getFieldByName(stream.getSchema(),
              leftKeyFieldName).get());
    }
    return stream;
  }


  private KsqlTopicSerDe getResultTopicSerde(final PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) node;
      return structuredDataSourceNode.getStructuredDataSource().getKsqlTopic().getKsqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;

      return getResultTopicSerde(joinNode.getLeft());
    } else {
      return getResultTopicSerde(node.getSources().get(0));
    }
  }


  public StreamsBuilder getBuilder() {
    return builder;
  }

  public OutputNode getPlanSink() {
    return planSink;
  }

  private void addAvroSchemaToResultTopic(final KsqlStructuredDataOutputNode.Builder builder,
                                          final KsqlStructuredDataOutputNode outputNode,
                                          final Schema schema) {
    final KsqlAvroTopicSerDe ksqlAvroTopicSerDe =
        new KsqlAvroTopicSerDe(metastoreUtil.buildAvroSchema(schema,
            outputNode.getKsqlTopic().getName()));
    builder.withKsqlTopic(new KsqlTopic(outputNode.getKsqlTopic()
        .getName(),
        outputNode.getKsqlTopic().getKafkaTopicName(),
        ksqlAvroTopicSerDe));
  }

  private SchemaKStream aggregateReKey(final AggregateNode aggregateNode,
                                       final SchemaKStream sourceSchemaKStream) {
    StringBuilder aggregateKeyName = new StringBuilder();
    List<Integer> newKeyIndexes = new ArrayList<>();
    boolean addSeparator = false;
    for (Expression groupByExpr : aggregateNode.getGroupByExpressions()) {
      if (addSeparator) {
        aggregateKeyName.append("|+|");
      } else {
        addSeparator = true;
      }
      aggregateKeyName.append(groupByExpr.toString());
      newKeyIndexes.add(getIndexInSchema(groupByExpr.toString(), sourceSchemaKStream.getSchema()));
    }

    KStream rekeyedKStream = sourceSchemaKStream.getKstream().selectKey((KeyValueMapper<String, GenericRow, String>) (key, value) -> {
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

    return new SchemaKStream(sourceSchemaKStream.getSchema(), rekeyedKStream, newKeyField,
        Collections.singletonList(sourceSchemaKStream), SchemaKStream.Type.REKEY, ksqlFunctionRegistry);
  }

  private int getIndexInSchema(final String fieldName, final Schema schema) {
    List<Field> fields = schema.fields();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (field.name().equals(fieldName)) {
        return i;
      }
    }
    throw new KsqlException("Couldn't find field with name="
        + fieldName
        + " in schema. fields="
        + fields);
  }

  private int getTimeStampColumnIndex(final Schema schema, final Field timestampField) {
    String timestampFieldName = timestampField.name();
    if (timestampFieldName.contains(".")) {
      for (int i = 2; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
        if (field.name().contains(".")) {
          if (timestampFieldName.equals(field.name())) {
            return i - 2;
          }
        } else {
          if (timestampFieldName
              .substring(timestampFieldName.indexOf(".") + 1).equals(field.name())) {
            return i - 2;
          }
        }
      }
    } else {
      for (int i = 2; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
        if (field.name().contains(".")) {
          if (timestampFieldName.equals(field.name().substring(field.name().indexOf(".") + 1))) {
            return i - 2;
          }
        } else {
          if (timestampFieldName.equals(field.name())) {
            return i - 2;
          }
        }
      }
    }
    return -1;
  }

}

