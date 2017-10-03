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
import io.confluent.ksql.function.KsqlFunctions;
import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.function.udaf.KudafInitializer;
import io.confluent.ksql.metastore.KsqlStream;
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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PhysicalPlanBuilder {

  private static final Logger log = LoggerFactory.getLogger(PhysicalPlanBuilder.class);

  private final KStreamBuilder builder;
  private final KsqlConfig ksqlConfig;
  private final KafkaTopicClient kafkaTopicClient;

  private OutputNode planSink = null;

  public PhysicalPlanBuilder(final KStreamBuilder builder, final KsqlConfig ksqlConfig, KafkaTopicClient kafkaTopicClient) {
    this.builder = builder;
    this.ksqlConfig = ksqlConfig;
    this.kafkaTopicClient = kafkaTopicClient;
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
      AggregateNode aggregateNode = (AggregateNode) planNode;
      SchemaKStream aggregateSchemaStream = buildAggregate(aggregateNode, propsMap);
      return aggregateSchemaStream;
    } else if (planNode instanceof ProjectNode) {
      ProjectNode projectNode = (ProjectNode) planNode;
      SchemaKStream projectedSchemaStream = buildProject(projectNode, propsMap);
      return projectedSchemaStream;
    } else if (planNode instanceof FilterNode) {
      FilterNode filterNode = (FilterNode) planNode;
      SchemaKStream filteredSchemaStream = buildFilter(filterNode, propsMap);
      return filteredSchemaStream;
    } else if (planNode instanceof OutputNode) {
      OutputNode outputNode = (OutputNode) planNode;
      SchemaKStream outputSchemaStream = buildOutput(outputNode, propsMap);
      return outputSchemaStream;
    }
    throw new KsqlException(
        "Unsupported logical plan node: " + planNode.getId() + " , Type: " + planNode.getClass()
            .getName());
  }

  private SchemaKStream buildOutput(final OutputNode outputNode, Map<String, Object> propsMap)
      throws Exception {
    SchemaKStream schemaKStream = kafkaStreamsDsl(outputNode.getSource());
    Set<Integer> rowkeyIndexes = SchemaUtil.getRowTimeRowKeyIndexes(outputNode.getSchema());
    if (outputNode instanceof KsqlStructuredDataOutputNode) {
      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode = (KsqlStructuredDataOutputNode)
          outputNode;
      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNodeNoRowKey = new
          KsqlStructuredDataOutputNode(
              ksqlStructuredDataOutputNode.getId(),
              ksqlStructuredDataOutputNode.getSource(),
              SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
                  ksqlStructuredDataOutputNode.getSchema()),
              ksqlStructuredDataOutputNode.getTimestampField(),
              ksqlStructuredDataOutputNode.getKeyField(),
              ksqlStructuredDataOutputNode.getKsqlTopic(),
              ksqlStructuredDataOutputNode.getKafkaTopicName(),
              ksqlStructuredDataOutputNode.getOutputProperties(),
              ksqlStructuredDataOutputNode.getLimit());
      if (ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic()
          .getKsqlTopicSerDe() instanceof KsqlAvroTopicSerDe) {
        KsqlAvroTopicSerDe ksqlAvroTopicSerDe =
            (KsqlAvroTopicSerDe) ksqlStructuredDataOutputNodeNoRowKey
            .getKsqlTopic().getKsqlTopicSerDe();
        ksqlStructuredDataOutputNodeNoRowKey =
            addAvroSchemaToResultTopic(ksqlStructuredDataOutputNodeNoRowKey);
      }

      Map<String, Object> outputProperties = ksqlStructuredDataOutputNodeNoRowKey
          .getOutputProperties();
      if (outputProperties.containsKey(KsqlConfig.SINK_NUMBER_OF_PARTITIONS)) {
        ksqlConfig.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS,
                       outputProperties.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS));
      }
      if (outputProperties.containsKey(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS)) {
        ksqlConfig.put(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS,
                       outputProperties.get(KsqlConfig.SINK_NUMBER_OF_REPLICATIONS));
      }
      SchemaKStream resultSchemaStream = schemaKStream;
      if (!(resultSchemaStream instanceof SchemaKTable)) {
        resultSchemaStream = new SchemaKStream(ksqlStructuredDataOutputNode.getSchema(),
                                               schemaKStream.getKstream(),
                                               ksqlStructuredDataOutputNode
                                                   .getKeyField(),
                                               Arrays.asList(schemaKStream),
                                               SchemaKStream.Type.SINK
        );

        if (outputProperties.containsKey(DdlConfig.PARTITION_BY_PROPERTY)) {
          String keyFieldName = outputProperties.get(DdlConfig.PARTITION_BY_PROPERTY).toString();
          Optional<Field> keyField = SchemaUtil.getFieldByName(
              resultSchemaStream.getSchema(), keyFieldName);
          if (!keyField.isPresent()) {
            throw new KsqlException(String.format("Column %s does not exist in the result schema."
                                                  + " Error in Partition By clause.",
                                                  keyFieldName));
          }
          resultSchemaStream = resultSchemaStream.selectKey(keyField.get());

          ksqlStructuredDataOutputNodeNoRowKey = new
              KsqlStructuredDataOutputNode(
              ksqlStructuredDataOutputNodeNoRowKey.getId(),
              ksqlStructuredDataOutputNodeNoRowKey.getSource(),
              ksqlStructuredDataOutputNodeNoRowKey.getSchema(),
              ksqlStructuredDataOutputNodeNoRowKey.getTimestampField(),
              keyField.get(),
              ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic(),
              ksqlStructuredDataOutputNodeNoRowKey.getKafkaTopicName(),
              ksqlStructuredDataOutputNodeNoRowKey.getOutputProperties(),
              ksqlStructuredDataOutputNodeNoRowKey.getLimit());
        }
      }

      resultSchemaStream = resultSchemaStream.into(
          ksqlStructuredDataOutputNodeNoRowKey.getKafkaTopicName(),
          SerDeUtil.getRowSerDe(
              ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic().getKsqlTopicSerDe(),
                       ksqlStructuredDataOutputNodeNoRowKey.getSchema()),
          rowkeyIndexes,
          ksqlConfig,
          kafkaTopicClient);


      KsqlStructuredDataOutputNode ksqlStructuredDataOutputNodeWithRowkey = new
          KsqlStructuredDataOutputNode(
          ksqlStructuredDataOutputNodeNoRowKey.getId(),
          ksqlStructuredDataOutputNodeNoRowKey.getSource(),
          SchemaUtil.addImplicitRowTimeRowKeyToSchema(
              ksqlStructuredDataOutputNodeNoRowKey.getSchema()),
          ksqlStructuredDataOutputNodeNoRowKey.getTimestampField(),
          ksqlStructuredDataOutputNodeNoRowKey.getKeyField(),
          ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic(),
          ksqlStructuredDataOutputNodeNoRowKey.getKafkaTopicName(),
          ksqlStructuredDataOutputNodeNoRowKey.getOutputProperties(),
          ksqlStructuredDataOutputNodeNoRowKey.getLimit()
      );
      this.planSink = ksqlStructuredDataOutputNodeWithRowkey;
      return resultSchemaStream;
    } else if (outputNode instanceof KsqlBareOutputNode) {
      this.planSink = outputNode;
      return schemaKStream.toQueue(outputNode.getLimit());
    }

    throw new KsqlException("Unsupported output logical node: " + outputNode.getClass().getName());
  }

  private SchemaKStream buildAggregate(final AggregateNode aggregateNode,
                                       Map<String, Object> propsMap) throws Exception {

    StructuredDataSourceNode streamSourceNode = aggregateNode.getTheSourceNode();

    SchemaKStream sourceSchemaKStream = kafkaStreamsDsl(aggregateNode.getSource());

    SchemaKStream rekeyedSchemaKStream = aggregateReKey(aggregateNode, sourceSchemaKStream);

    // Pre aggregate computations
    List<Pair<String, Expression>> aggArgExpansionList = new ArrayList<>();
    Map<String, Integer> expressionNames = new HashMap<>();
    for (Expression expression: aggregateNode.getRequiredColumnList()) {
      if (!expressionNames.containsKey(expression.toString())) {
        expressionNames.put(expression.toString(), aggArgExpansionList.size());
        aggArgExpansionList.add(new Pair<>(expression.toString(), expression));
      }
    }
    for (Expression expression: aggregateNode.getAggregateFunctionArguments()) {
      if (!expressionNames.containsKey(expression.toString())) {
        expressionNames.put(expression.toString(), aggArgExpansionList.size());
        aggArgExpansionList.add(new Pair<>(expression.toString(), expression));
      }
    }

    SchemaKStream aggregateArgExpanded = rekeyedSchemaKStream.select(aggArgExpansionList);

    Serde<GenericRow> genericRowSerde =
        SerDeUtil.getRowSerDe(streamSourceNode.getStructuredDataSource()
            .getKsqlTopic()
            .getKsqlTopicSerDe(),
        aggregateArgExpanded.getSchema());
    SchemaKGroupedStream schemaKGroupedStream =
        aggregateArgExpanded.groupByKey(Serdes.String(), genericRowSerde);

    // Aggregate computations
    Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    SchemaBuilder aggregateSchema = SchemaBuilder.struct();

    List resultColumns = new ArrayList();
    int nonAggColumnIndex = 0;
    for (Expression expression: aggregateNode.getRequiredColumnList()) {
      String exprStr = expression.toString();
      int index = getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
      aggValToValColumnMap.put(nonAggColumnIndex, index);
      nonAggColumnIndex++;
      resultColumns.add("");
      Field field = aggregateArgExpanded.getSchema().fields().get(index);
      aggregateSchema.field(field.name(), field.schema());
    }
    int udafIndexInAggSchema = resultColumns.size();
    for (FunctionCall functionCall: aggregateNode.getFunctionList()) {
      KsqlAggregateFunction aggregateFunctionInfo = KsqlFunctions.getAggregateFunction(functionCall
              .getName()
              .toString(),
              functionCall
              .getArguments(), aggregateArgExpanded.getSchema());
      int udafIndex = expressionNames.get(functionCall.getArguments().get(0).toString());
      KsqlAggregateFunction aggregateFunction = aggregateFunctionInfo.getClass()
          .getDeclaredConstructor(Integer.class).newInstance(udafIndex);
      aggValToAggFunctionMap.put(udafIndexInAggSchema, aggregateFunction);
      resultColumns.add(aggregateFunction.getIntialValue());

      udafIndexInAggSchema++;
      aggregateSchema.field("AGG_COL_"
                            + udafIndexInAggSchema, aggregateFunction.getReturnType());
    }

    Serde<GenericRow> aggValueGenericRowSerde = SerDeUtil.getRowSerDe(streamSourceNode
            .getStructuredDataSource()
            .getKsqlTopic()
            .getKsqlTopicSerDe(),
        aggregateSchema);

    SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        new KudafInitializer(resultColumns),
        new KudafAggregator(aggValToAggFunctionMap,
                            aggValToValColumnMap), aggregateNode.getWindowExpression(),
        aggValueGenericRowSerde, "KSQL_Agg_Query_" + System.currentTimeMillis());

    // Post aggregate computations
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    List<Field> fields = schemaKTable.getSchema().fields();
    for (int i = 0; i < aggregateNode.getRequiredColumnList().size(); i++) {
      schemaBuilder.field(fields.get(i).name(), fields.get(i).schema());
    }
    for (int aggFunctionVarSuffix = 0;
         aggFunctionVarSuffix < aggregateNode.getFunctionList().size(); aggFunctionVarSuffix++) {
      Schema fieldSchema;
      String udafName = aggregateNode.getFunctionList().get(aggFunctionVarSuffix).getName()
          .getSuffix();
      KsqlAggregateFunction aggregateFunction = KsqlFunctions.getAggregateFunction(udafName,
                                                                                   aggregateNode
              .getFunctionList()
              .get(aggFunctionVarSuffix).getArguments(), schemaKTable.getSchema());
      fieldSchema = aggregateFunction.getReturnType();
      schemaBuilder.field(AggregateExpressionRewriter.AGGREGATE_FUNCTION_VARIABLE_PREFIX
          + aggFunctionVarSuffix, fieldSchema);
    }

    Schema aggStageSchema = schemaBuilder.build();

    SchemaKTable finalSchemaKTable = new SchemaKTable(aggStageSchema, schemaKTable.getKtable(),
                                                      schemaKTable.getKeyField(),
                                                      schemaKTable.getSourceSchemaKStreams(),
                                                      schemaKTable.isWindowed(),
                                                      SchemaKStream.Type.AGGREGATE);

    if (aggregateNode.getHavingExpressions() != null) {
      finalSchemaKTable = finalSchemaKTable.filter(aggregateNode.getHavingExpressions());
    }

    SchemaKTable finalResult = finalSchemaKTable.select(aggregateNode.getFinalSelectExpressions());

    return finalResult;
  }

  private SchemaKStream buildProject(final ProjectNode projectNode, Map<String, Object> propsMap)
      throws Exception {
    SchemaKStream projectedSchemaStream =
        kafkaStreamsDsl(projectNode.getSource()).select(projectNode.getProjectNameExpressionPairList());
    return projectedSchemaStream;
  }


  private SchemaKStream buildFilter(final FilterNode filterNode, Map<String, Object> propsMap)
      throws Exception {
    SchemaKStream
        filteredSchemaKStream =
        kafkaStreamsDsl(filterNode.getSource()).filter(filterNode.getPredicate());
    return filteredSchemaKStream;
  }

  private SchemaKStream buildSource(final SourceNode sourceNode, Map<String, Object> props) {

    TopologyBuilder.AutoOffsetReset autoOffsetReset = null;
    if (props.containsKey(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)) {
      if (props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG).toString()
          .equalsIgnoreCase("EARLIEST")) {
        autoOffsetReset = TopologyBuilder.AutoOffsetReset.EARLIEST;
      } else if (props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG).toString()
          .equalsIgnoreCase("LATEST")) {
        autoOffsetReset = TopologyBuilder.AutoOffsetReset.LATEST;
      }
    }


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

      Serde<GenericRow> genericRowSerdeAfterRead =
          SerDeUtil.getRowSerDe(structuredDataSourceNode.getStructuredDataSource()
                  .getKsqlTopic().getKsqlTopicSerDe(),
              structuredDataSourceNode.getSchema());

      if (structuredDataSourceNode.getDataSourceType()
          == StructuredDataSource.DataSourceType.KTABLE) {

        KsqlTable ksqlTable = (KsqlTable) structuredDataSourceNode.getStructuredDataSource();
        KTable ktable;
        if (ksqlTable.isWindowed()) {
          KStream
              kstream =
              builder
                  .stream(autoOffsetReset, new WindowedSerde(), genericRowSerde,
                      ksqlTable.getKsqlTopic().getKafkaTopicName())
                  .map(new KeyValueMapper<Windowed<String>, GenericRow, KeyValue<Windowed<String>,
                      GenericRow>>() {
                    @Override
                    public KeyValue<Windowed<String>, GenericRow> apply(
                        Windowed<String> key, GenericRow row) {
                      if (row != null) {
                        row.getColumns().add(0,
                                             String.format("%s : Window{start=%d end=-}", key
                                                 .key(), key.window().start()));

                      }
                      return new KeyValue<>(key, row);
                    }
                  });
          kstream = addTimestampColumn(kstream);
          ktable = kstream
              .groupByKey(new WindowedSerde(), genericRowSerdeAfterRead)
              .reduce(new Reducer<GenericRow>() {
                @Override
                public GenericRow apply(GenericRow aggValue, GenericRow newValue) {
                  return newValue;
                }
              }, ksqlTable.getStateStoreName());
        } else {
          KStream
              kstream =
              builder
                  .stream(autoOffsetReset, Serdes.String(), genericRowSerde,
                      ksqlTable.getKsqlTopic().getKafkaTopicName())
                  .map((KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>) (key, row) -> {
                    if (row != null) {
                      row.getColumns().add(0, key);

                    }
                    return new KeyValue<>(key, row);
                  });
          kstream = addTimestampColumn(kstream);
          ktable = kstream.groupByKey(Serdes.String(), genericRowSerdeAfterRead)
                  .reduce((Reducer<GenericRow>) (aggValue, newValue) -> newValue, ksqlTable.getStateStoreName());
        }

        return new SchemaKTable(sourceNode.getSchema(), ktable,
                                sourceNode.getKeyField(), new ArrayList<>(),
                                ksqlTable.isWindowed(),
                                SchemaKStream.Type.SOURCE);
      }
      KsqlStream ksqlStream = (KsqlStream) structuredDataSourceNode.getStructuredDataSource();
      KStream
          kstream =
          builder
              .stream(Serdes.String(), genericRowSerde,
                  ksqlStream.getKsqlTopic().getKafkaTopicName())
              .map((KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>>) (key, row) -> {
                if (row != null) {
                  row.getColumns().add(0, key);

                }
                return new KeyValue<>(key, row);
              });
      kstream = addTimestampColumn(kstream);
      return new SchemaKStream(sourceNode.getSchema(), kstream,
                               sourceNode.getKeyField(), new ArrayList<>(),
                               SchemaKStream.Type.SOURCE);
    }
    throw new KsqlException("Unsupported source logical node: " + sourceNode.getClass().getName());
  }

  private SchemaKStream buildJoin(final JoinNode joinNode, Map<String, Object> propsMap)
      throws Exception {
    SchemaKStream leftSchemaKStream = kafkaStreamsDsl(joinNode.getLeft());

    propsMap.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
              TopologyBuilder.AutoOffsetReset.EARLIEST.toString());
    SchemaKStream rightSchemaKStream = kafkaStreamsDsl(joinNode.getRight(), propsMap);
    if (rightSchemaKStream instanceof SchemaKTable) {
      SchemaKTable rightSchemaKTable = (SchemaKTable) rightSchemaKStream;

      if (
          leftSchemaKStream.getKeyField() == null ||
          !leftSchemaKStream.getKeyField().name().equals(joinNode.getLeftKeyFieldName())) {
        leftSchemaKStream =
            leftSchemaKStream.selectKey(SchemaUtil.getFieldByName(leftSchemaKStream.getSchema(),
                joinNode.getLeftKeyFieldName()).get());
      }
      SchemaKStream joinSchemaKStream;
      switch (joinNode.getType()) {
        case LEFT:
          KsqlTopicSerDe joinSerDe = getResultTopicSerde(joinNode);
          String joinKeyFieldName = (joinNode.getLeftAlias() + "." + leftSchemaKStream
              .getKeyField().name());
          joinSchemaKStream =
              leftSchemaKStream.leftJoin(rightSchemaKTable, joinNode.getSchema(),
                  joinNode.getSchema().field(joinKeyFieldName), joinSerDe);
          break;
        default:
          throw new KsqlException("Join type is not supportd yet: " + joinNode.getType());
      }
      return joinSchemaKStream;
    }

    throw new KsqlException("Unsupported join logical node: Left: "
                            + joinNode.getLeft() + " , Right: " + joinNode.getRight());
  }

  private KsqlTopicSerDe getResultTopicSerde(final PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) node;
      return structuredDataSourceNode.getStructuredDataSource().getKsqlTopic().getKsqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;

      KsqlTopicSerDe leftTopicSerDe = getResultTopicSerde(joinNode.getLeft());
      return leftTopicSerDe;
    } else {
      return getResultTopicSerde(node.getSources().get(0));
    }
  }


  public KStreamBuilder getBuilder() {
    return builder;
  }

  public OutputNode getPlanSink() {
    return planSink;
  }

  private KsqlStructuredDataOutputNode addAvroSchemaToResultTopic(
      final KsqlStructuredDataOutputNode ksqlStructuredDataOutputNode) {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    String
        avroSchema =
        metastoreUtil.buildAvroSchema(ksqlStructuredDataOutputNode.getSchema(),
            ksqlStructuredDataOutputNode.getKsqlTopic().getName());
    KsqlAvroTopicSerDe ksqlAvroTopicSerDe =
        new KsqlAvroTopicSerDe(avroSchema);
    KsqlTopic newKsqlTopic = new KsqlTopic(ksqlStructuredDataOutputNode.getKsqlTopic()
        .getName(), ksqlStructuredDataOutputNode
        .getKsqlTopic().getKafkaTopicName(), ksqlAvroTopicSerDe);

    KsqlStructuredDataOutputNode newKsqlStructuredDataOutputNode = new KsqlStructuredDataOutputNode(
        ksqlStructuredDataOutputNode.getId(), ksqlStructuredDataOutputNode.getSource(),
        ksqlStructuredDataOutputNode.getSchema(), ksqlStructuredDataOutputNode.getTimestampField(),
        ksqlStructuredDataOutputNode.getKeyField(), newKsqlTopic,
        ksqlStructuredDataOutputNode.getKafkaTopicName(),
        ksqlStructuredDataOutputNode.getOutputProperties(),
        ksqlStructuredDataOutputNode.getLimit());
    return newKsqlStructuredDataOutputNode;
  }

  private SchemaKStream aggregateReKey(final AggregateNode aggregateNode,
                                       final SchemaKStream sourceSchemaKStream) {
    String aggregateKeyName = "";
    List<Integer> newKeyIndexes = new ArrayList<>();
    boolean addSeparator = false;
    for (Expression groupByExpr : aggregateNode.getGroupByExpressions()) {
      if (addSeparator) {
        aggregateKeyName += "|+|";
      } else {
        addSeparator = true;
      }
      aggregateKeyName += groupByExpr.toString();
      newKeyIndexes.add(getIndexInSchema(groupByExpr.toString(), sourceSchemaKStream.getSchema()));
    }

    KStream rekeyedKStream = sourceSchemaKStream.getKstream().selectKey(new KeyValueMapper<String, GenericRow, String>() {

      @Override
      public String apply(String key, GenericRow value) {
        String newKey = "";
        boolean addSeparator = false;
        for (int index : newKeyIndexes) {
          if (addSeparator) {
            newKey += "|+|";
          } else {
            addSeparator = true;
          }
          newKey += String.valueOf(value.getColumns().get(index));
        }
        return newKey;
      }
    });

    Field newKeyField = new Field(aggregateKeyName, -1, Schema.STRING_SCHEMA);

    return new SchemaKStream(sourceSchemaKStream.getSchema(), rekeyedKStream, newKeyField,
                             Arrays.asList(sourceSchemaKStream), SchemaKStream.Type.REKEY);
  }

  private int getIndexInSchema(final String fieldName, final Schema schema) {
    List<Field> fields = schema.fields();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (field.name().equals(fieldName)) {
        return i;
      }
    }
    return -1;
  }

  private KStream addTimestampColumn(final KStream kstream) {
    return kstream.transformValues(new ValueTransformerSupplier<GenericRow, GenericRow>() {
      @Override
      public ValueTransformer<GenericRow, GenericRow> get() {
        return new ValueTransformer<GenericRow, GenericRow>() {
          ProcessorContext processorContext;
          @Override
          public void init(ProcessorContext processorContext) {
            this.processorContext = processorContext;
          }

          @Override
          public GenericRow transform(GenericRow row) {
            if (row != null) {
              row.getColumns().add(0, processorContext.timestamp());

            }
            return row;
          }

          @Override
          public GenericRow punctuate(long l) {
            return null;
          }

          @Override
          public void close() {

          }
        };
      }
    });
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

