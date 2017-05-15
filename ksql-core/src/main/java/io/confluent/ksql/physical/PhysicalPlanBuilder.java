/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.physical;

import io.confluent.ksql.function.KSQLAggregateFunction;
import io.confluent.ksql.function.KSQLFunctions;
import io.confluent.ksql.function.udaf.KUDAFAggregator;
import io.confluent.ksql.function.udaf.KUDAFInitializer;
import io.confluent.ksql.metastore.KSQLStream;
import io.confluent.ksql.metastore.KSQLTable;
import io.confluent.ksql.metastore.KSQLTopic;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.KSQLBareOutputNode;
import io.confluent.ksql.planner.plan.KSQLStructuredDataOutputNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.SourceNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.KSQLTopicSerDe;
import io.confluent.ksql.serde.avro.KSQLAvroTopicSerDe;
import io.confluent.ksql.serde.avro.KSQLGenericRowAvroSerializer;
import io.confluent.ksql.structured.SchemaKGroupedStream;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;
import io.confluent.ksql.util.WindowedSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PhysicalPlanBuilder {

  KStreamBuilder builder;
  OutputNode planSink = null;

  public PhysicalPlanBuilder(final KStreamBuilder builder) {

    this.builder = builder;
  }

  public SchemaKStream buildPhysicalPlan(final PlanNode logicalPlanRoot) throws Exception {
    return kafkaStreamsDSL(logicalPlanRoot);
  }

  private SchemaKStream kafkaStreamsDSL(final PlanNode planNode) throws Exception {
    if (planNode instanceof SourceNode) {
      return buildSource((SourceNode) planNode);
    } else if (planNode instanceof JoinNode) {
      return buildJoin((JoinNode) planNode);
    } else if (planNode instanceof AggregateNode) {
      AggregateNode aggregateNode = (AggregateNode) planNode;
      SchemaKStream aggregateSchemaStream = buildAggregate(aggregateNode);
      return aggregateSchemaStream;
    } else if (planNode instanceof ProjectNode) {
      ProjectNode projectNode = (ProjectNode) planNode;
      SchemaKStream projectedSchemaStream = buildProject(projectNode);
      return projectedSchemaStream;
    } else if (planNode instanceof FilterNode) {
      FilterNode filterNode = (FilterNode) planNode;
      SchemaKStream filteredSchemaStream = buildFilter(filterNode);
      return filteredSchemaStream;
    } else if (planNode instanceof OutputNode) {
      OutputNode outputNode = (OutputNode) planNode;
      SchemaKStream outputSchemaStream = buildOutput(outputNode);
      return outputSchemaStream;
    }
    throw new KSQLException(
        "Unsupported logical plan node: " + planNode.getId() + " , Type: " + planNode.getClass()
            .getName());
  }

  private SchemaKStream buildOutput(final OutputNode outputNode) throws Exception {
    SchemaKStream schemaKStream = kafkaStreamsDSL(outputNode.getSource());
    if (outputNode instanceof KSQLStructuredDataOutputNode) {
      KSQLStructuredDataOutputNode ksqlStructuredDataOutputNode = (KSQLStructuredDataOutputNode)
          outputNode;
      KSQLStructuredDataOutputNode ksqlStructuredDataOutputNodeNoRowKey = new
          KSQLStructuredDataOutputNode(
              ksqlStructuredDataOutputNode.getId(),
              ksqlStructuredDataOutputNode.getSource(),
              SchemaUtil.removeImplicitRowKeyFromSchema(ksqlStructuredDataOutputNode.getSchema()),
              ksqlStructuredDataOutputNode.getKsqlTopic(),
              ksqlStructuredDataOutputNode.getKafkaTopicName()
                                       );
      if (ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic()
          .getKsqlTopicSerDe() instanceof KSQLAvroTopicSerDe) {
        KSQLAvroTopicSerDe ksqlAvroTopicSerDe = (KSQLAvroTopicSerDe) ksqlStructuredDataOutputNodeNoRowKey
            .getKsqlTopic().getKsqlTopicSerDe();
        ksqlStructuredDataOutputNodeNoRowKey = addAvroSchemaToResultTopic(ksqlStructuredDataOutputNodeNoRowKey,
                                                                 ksqlAvroTopicSerDe
                                                                     .getSchemaFilePath());
      }

      Set<Integer> rowkeyIndexes = SchemaUtil.getRowKeyIndexes(ksqlStructuredDataOutputNode.getSchema());
      SchemaKStream resultSchemaStream = schemaKStream.into(ksqlStructuredDataOutputNodeNoRowKey
                                                                .getKafkaTopicName(), SerDeUtil
          .getRowSerDe(ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic().getKsqlTopicSerDe(),
                       ksqlStructuredDataOutputNodeNoRowKey.getSchema()), rowkeyIndexes);


      KSQLStructuredDataOutputNode ksqlStructuredDataOutputNodeWithRowkey = new
          KSQLStructuredDataOutputNode(
          ksqlStructuredDataOutputNodeNoRowKey.getId(),
          ksqlStructuredDataOutputNodeNoRowKey.getSource(),
          SchemaUtil.addImplicitKeyToSchema(ksqlStructuredDataOutputNodeNoRowKey.getSchema()),
          ksqlStructuredDataOutputNodeNoRowKey.getKsqlTopic(),
          ksqlStructuredDataOutputNodeNoRowKey.getKafkaTopicName()
      );
      this.planSink = ksqlStructuredDataOutputNodeWithRowkey;
      return resultSchemaStream;
    } else if (outputNode instanceof KSQLBareOutputNode) {
      SchemaKStream resultSchemaStream = schemaKStream.toQueue();
      KSQLBareOutputNode ksqlBareOutputNode = (KSQLBareOutputNode) outputNode;
      this.planSink = ksqlBareOutputNode;
      return resultSchemaStream;
    }
    throw new KSQLException("Unsupported output logical node: " + outputNode.getClass().getName());
  }

  private SchemaKStream buildAggregate(final AggregateNode aggregateNode) throws Exception {

    StructuredDataSourceNode streamSourceNode = aggregateNode.getTheSourceNode();

    SchemaKStream sourceSchemaKStream = kafkaStreamsDSL(aggregateNode.getSource());

    SchemaKStream rekeyedSchemaKStream = aggregateReKey(aggregateNode, sourceSchemaKStream);

    // Pre aggregate computations
    List<Expression> aggArgExpansionList = new ArrayList<>();
    Map<String, Integer> expressionNames = new HashMap<>();
    for (Expression expression: aggregateNode.getRequiredColumnList()) {
      if (!expressionNames.containsKey(expression.toString())) {
        expressionNames.put(expression.toString(), aggArgExpansionList.size());
        aggArgExpansionList.add(expression);
      }
    }
    for (Expression expression: aggregateNode.getAggregateFunctionArguments()) {
      if (!expressionNames.containsKey(expression.toString())) {
        expressionNames.put(expression.toString(), aggArgExpansionList.size());
        aggArgExpansionList.add(expression);
      }
    }

    SchemaKStream aggregateArgExpanded = rekeyedSchemaKStream.select(aggArgExpansionList);

    Serde<GenericRow> genericRowSerde = SerDeUtil.getRowSerDe(streamSourceNode.getStructuredDataSource()
                                                                  .getKsqlTopic()
                                                                  .getKsqlTopicSerDe(),
                                                              aggregateArgExpanded.getSchema());
    SchemaKGroupedStream schemaKGroupedStream = aggregateArgExpanded.groupByKey(Serdes.String(), genericRowSerde);

    // Aggregate computations
    Map<Integer, KSQLAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
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
      KSQLAggregateFunction aggregateFunctionInfo = KSQLFunctions.getAggregateFunction(functionCall
                                                                                      .getName()
                                                                                     .toString(),
                                                                                 functionCall
                                                                                     .getArguments(), aggregateArgExpanded.getSchema());
      int udafIndex = expressionNames.get(functionCall.getArguments().get(0).toString());
      KSQLAggregateFunction aggregateFunction = aggregateFunctionInfo.getClass()
          .getDeclaredConstructor(Integer.class).newInstance(udafIndex);
      aggValToAggFunctionMap.put(udafIndexInAggSchema, aggregateFunction);
      resultColumns.add(aggregateFunction.getIntialValue());

      udafIndexInAggSchema++;
      aggregateSchema.field("AGG_COL_" + udafIndexInAggSchema, aggregateFunction.getReturnType());
    }

    Serde<GenericRow> aggValueGenericRowSerde = SerDeUtil.getRowSerDe(streamSourceNode
                                                                   .getStructuredDataSource()
                                                                  .getKsqlTopic()
                                                                  .getKsqlTopicSerDe(),
                                                                      aggregateSchema);

    SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        new KUDAFInitializer(resultColumns),
        new KUDAFAggregator(aggValToAggFunctionMap, aggValToValColumnMap), aggregateNode.getWindowExpression(), aggValueGenericRowSerde, "KSQL_Agg_Query_" + System.currentTimeMillis());

    // Post aggregate computations
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    List<Field> fields = schemaKTable.getSchema().fields();
    for (int i = 0; i < aggregateNode.getRequiredColumnList().size(); i++) {
      schemaBuilder.field(fields.get(i).name(), fields.get(i).schema());
    }
    for (int aggFunctionVarSuffix = 0; aggFunctionVarSuffix < aggregateNode.getFunctionList().size(); aggFunctionVarSuffix++) {
      Schema fieldSchema;
      String udafName = aggregateNode.getFunctionList().get(aggFunctionVarSuffix).getName()
          .getSuffix();
      KSQLAggregateFunction aggregateFunction = KSQLFunctions.getAggregateFunction(udafName,
                                                                                 aggregateNode
                                                                                     .getFunctionList().get(aggFunctionVarSuffix).getArguments(), schemaKTable.getSchema());
      fieldSchema = aggregateFunction.getReturnType();
      schemaBuilder.field(AggregateExpressionRewriter.AGGREGATE_FUNCTION_VARIABLE_PREFIX
                          + aggFunctionVarSuffix, fieldSchema);
    }

    Schema aggStageSchema = schemaBuilder.build();

    SchemaKTable finalSchemaKTable = new SchemaKTable(aggStageSchema, schemaKTable.getkTable(),
                                                      schemaKTable.getKeyField(),
                                                      schemaKTable.getSourceSchemaKStreams(), true);

    if (aggregateNode.getHavingExpressions() != null) {
      finalSchemaKTable = finalSchemaKTable.filter(aggregateNode.getHavingExpressions());
    }

    SchemaKTable finalResult = finalSchemaKTable.select(aggregateNode.getFinalSelectExpressions());

    return finalResult;
  }

  private SchemaKStream buildProject(final ProjectNode projectNode) throws Exception {
    SchemaKStream projectedSchemaStream = kafkaStreamsDSL(projectNode.getSource()).select(projectNode.getProjectExpressions());
    return projectedSchemaStream;
  }


  private SchemaKStream buildFilter(final FilterNode filterNode) throws Exception {
    SchemaKStream
        filteredSchemaKStream =
        kafkaStreamsDSL(filterNode.getSource()).filter(filterNode.getPredicate());
    return filteredSchemaKStream;
  }

  private SchemaKStream buildSource(final SourceNode sourceNode) {
    if (sourceNode instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) sourceNode;

      Serde<GenericRow>
          genericRowSerde =
          SerDeUtil.getRowSerDe(structuredDataSourceNode.getStructuredDataSource()
                                    .getKsqlTopic().getKsqlTopicSerDe(), structuredDataSourceNode.getSchema());

      if (structuredDataSourceNode.getDataSourceType()
          == StructuredDataSource.DataSourceType.KTABLE) {

        KSQLTable ksqlTable = (KSQLTable) structuredDataSourceNode.getStructuredDataSource();
        KTable kTable;
        if (ksqlTable.isWinidowed()) {
          KStream
              kStream =
              builder
                  .stream(new WindowedSerde(), genericRowSerde,
                          ksqlTable.getKsqlTopic().getKafkaTopicName())
                  .map(new KeyValueMapper<Windowed<String>, GenericRow, KeyValue<Windowed<String>,
                      GenericRow>>() {
                    @Override
                    public KeyValue<Windowed<String>, GenericRow> apply(Windowed<String> key, GenericRow row) {
                      row.getColumns().set(0, String.format("%s : Window{start=%s, end=%s}", key
                          .key(), key.window().start(), key.window().end()));
                      return new KeyValue<>(key, row);
                    }
                  });
          kTable = kStream.groupByKey(new WindowedSerde(), genericRowSerde).reduce(new Reducer<GenericRow>() {
            @Override
            public GenericRow apply(GenericRow aggValue, GenericRow newValue) {
              return newValue;
            }
          }, ksqlTable.getStateStoreName());
        } else {
          KStream
              kStream =
              builder
                  .stream(Serdes.String(), genericRowSerde,
                          ksqlTable.getKsqlTopic().getKafkaTopicName())
                  .map(new KeyValueMapper<String, GenericRow, KeyValue<String,
                      GenericRow>>() {
                    @Override
                    public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
                      row.getColumns().set(0, key);
                      return new KeyValue<>(key, row);
                    }
                  });
          kTable = kStream.groupByKey(Serdes.String(), genericRowSerde).reduce(new Reducer<GenericRow>() {
            @Override
            public GenericRow apply(GenericRow aggValue, GenericRow newValue) {
              return newValue;
            }
          }, ksqlTable.getStateStoreName());
        }

        return new SchemaKTable(sourceNode.getSchema(), kTable,
                                sourceNode.getKeyField(), new ArrayList<>(), ksqlTable.isWinidowed());
      }
      KSQLStream ksqlStream = (KSQLStream) structuredDataSourceNode.getStructuredDataSource();
      KStream
          kStream =
          builder
              .stream(Serdes.String(), genericRowSerde,
                      ksqlStream.getKsqlTopic().getKafkaTopicName())
              .map(new KeyValueMapper<String, GenericRow, KeyValue<String,
                  GenericRow>>() {
                @Override
                public KeyValue<String, GenericRow> apply(String key, GenericRow row) {
                  row.getColumns().set(0, key);
                  return new KeyValue<>(key, row);
                }
              });
      return new SchemaKStream(sourceNode.getSchema(), kStream,
                               sourceNode.getKeyField(), new ArrayList<>());
    }
    throw new KSQLException("Unsupported source logical node: " + sourceNode.getClass().getName());
  }

  private SchemaKStream buildJoin(final JoinNode joinNode) throws Exception {
    SchemaKStream leftSchemaKStream = kafkaStreamsDSL(joinNode.getLeft());
    SchemaKStream rightSchemaKStream = kafkaStreamsDSL(joinNode.getRight());
    if (rightSchemaKStream instanceof SchemaKTable) {
      SchemaKTable rightSchemaKTable = (SchemaKTable) rightSchemaKStream;
      if (!leftSchemaKStream.getKeyField().name().equals(joinNode.getLeftKeyFieldName())) {
        leftSchemaKStream =
            leftSchemaKStream.selectKey(SchemaUtil.getFieldByName(leftSchemaKStream.getSchema(),
                                                                  joinNode.getLeftKeyFieldName()));
      }
      SchemaKStream joinSchemaKStream;
      switch (joinNode.getType()) {
        case LEFT:
          KSQLTopicSerDe joinSerDe = getResultTopicSerde(joinNode);
          String joinKeyFieldName = (joinNode.getLeftAlias() + "." + leftSchemaKStream
              .getKeyField().name());
          joinSchemaKStream =
              leftSchemaKStream.leftJoin(rightSchemaKTable, joinNode.getSchema(),
                                         joinNode.getSchema().field(joinKeyFieldName), joinSerDe);
          break;
        default:
          throw new KSQLException("Join type is not supportd yet: " + joinNode.getType());
      }
      return joinSchemaKStream;
    }

    throw new KSQLException("Unsupported join logical node: Left: " + joinNode.getLeft() + " , Right: " + joinNode.getRight());
  }

  private KSQLTopicSerDe getResultTopicSerde(final PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) node;
      return structuredDataSourceNode.getStructuredDataSource().getKsqlTopic().getKsqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;

      KSQLTopicSerDe leftTopicSerDe = getResultTopicSerde(joinNode.getLeft());
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

  private KSQLStructuredDataOutputNode addAvroSchemaToResultTopic(final KSQLStructuredDataOutputNode
                                                                      ksqlStructuredDataOutputNode,
                                                                  final String avroSchemaFilePath) {
    String avroSchemaFilePathVal = avroSchemaFilePath;
    if (avroSchemaFilePath == null) {
      avroSchemaFilePathVal =
          KSQLGenericRowAvroSerializer.AVRO_SERDE_SCHEMA_DIRECTORY_DEFAULT + ksqlStructuredDataOutputNode
              .getKsqlTopic().getName() + ".avro";
    }
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    String
        avroSchema =
        metastoreUtil.buildAvroSchema(ksqlStructuredDataOutputNode.getSchema(),
                                      ksqlStructuredDataOutputNode.getKsqlTopic().getName());
    metastoreUtil.writeAvroSchemaFile(avroSchema, avroSchemaFilePathVal);
    KSQLAvroTopicSerDe ksqlAvroTopicSerDe = new KSQLAvroTopicSerDe(avroSchemaFilePathVal, avroSchema);
    KSQLTopic newKSQLTopic = new KSQLTopic(ksqlStructuredDataOutputNode.getKsqlTopic()
                                            .getName(), ksqlStructuredDataOutputNode
                                            .getKsqlTopic().getKafkaTopicName(), ksqlAvroTopicSerDe);

    KSQLStructuredDataOutputNode newKSQLStructuredDataOutputNode = new KSQLStructuredDataOutputNode(
        ksqlStructuredDataOutputNode.getId(), ksqlStructuredDataOutputNode.getSource(),
        ksqlStructuredDataOutputNode.getSchema(), newKSQLTopic,
        ksqlStructuredDataOutputNode.getKafkaTopicName());
    return newKSQLStructuredDataOutputNode;
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

    KStream rekeyedKStream = sourceSchemaKStream.getkStream().selectKey(new KeyValueMapper<String, GenericRow, String>() {

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
                             Arrays.asList(sourceSchemaKStream));
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
}
