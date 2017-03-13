/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.physical;

import io.confluent.kql.function.KQLAggregateFunction;
import io.confluent.kql.function.KQLFunction;
import io.confluent.kql.function.KQLFunctions;
import io.confluent.kql.function.udaf.KUDAFAggregator;
import io.confluent.kql.function.udaf.KUDAFInitializer;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetastoreUtil;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.rewrite.AggregateExpressionRewriter;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.planner.plan.AggregateNode;
import io.confluent.kql.planner.plan.FilterNode;
import io.confluent.kql.planner.plan.JoinNode;
import io.confluent.kql.planner.plan.KQLConsoleOutputNode;
import io.confluent.kql.planner.plan.KQLStructuredDataOutputNode;
import io.confluent.kql.planner.plan.OutputNode;
import io.confluent.kql.planner.plan.PlanNode;
import io.confluent.kql.planner.plan.ProjectNode;
import io.confluent.kql.planner.plan.SourceNode;
import io.confluent.kql.planner.plan.StructuredDataSourceNode;
import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.structured.SchemaKGroupedStream;
import io.confluent.kql.structured.SchemaKTable;
import io.confluent.kql.structured.SchemaKStream;

import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.SchemaUtil;
import io.confluent.kql.util.SerDeUtil;
import io.confluent.kql.util.KQLConfig;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    throw new KQLException(
        "Unsupported logical plan node: " + planNode.getId() + " , Type: " + planNode.getClass()
            .getName());
  }

  private SchemaKStream buildOutput(final OutputNode outputNode) throws Exception {
    SchemaKStream schemaKStream = kafkaStreamsDSL(outputNode.getSource());
    if (outputNode instanceof KQLStructuredDataOutputNode) {
      KQLStructuredDataOutputNode kqlStructuredDataOutputNode = (KQLStructuredDataOutputNode)
          outputNode;
      if (kqlStructuredDataOutputNode.getKqlTopic()
          .getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
        KQLAvroTopicSerDe kqlAvroTopicSerDe = (KQLAvroTopicSerDe) kqlStructuredDataOutputNode
            .getKqlTopic().getKqlTopicSerDe();
        kqlStructuredDataOutputNode = addAvroSchemaToResultTopic(kqlStructuredDataOutputNode,
                                                                 kqlAvroTopicSerDe
                                                                     .getSchemaFilePath());
      }
      SchemaKStream resultSchemaStream = schemaKStream.into(kqlStructuredDataOutputNode.getKafkaTopicName(), SerDeUtil.getRowSerDe(kqlStructuredDataOutputNode.getKqlTopic().getKqlTopicSerDe()));

      this.planSink = kqlStructuredDataOutputNode;
      return resultSchemaStream;
    } else if (outputNode instanceof KQLConsoleOutputNode) {
      SchemaKStream resultSchemaStream = schemaKStream.print();
      KQLConsoleOutputNode kqlConsoleOutputNode = (KQLConsoleOutputNode) outputNode;
      this.planSink = kqlConsoleOutputNode;
      return resultSchemaStream;
    }
    throw new KQLException("Unsupported output logical node: " + outputNode.getClass().getName());
  }

  private SchemaKStream buildAggregate(final AggregateNode aggregateNode) throws Exception {

    StructuredDataSourceNode sourceNode = (StructuredDataSourceNode) aggregateNode.getSource();
    Serde<GenericRow> genericRowSerde = SerDeUtil.getRowSerDe(sourceNode.getStructuredDataSource()
                                                                  .getKqlTopic()
                                                                  .getKqlTopicSerDe());
    SchemaKStream sourceSchemaKStream = kafkaStreamsDSL(aggregateNode.getSource());

    SchemaKStream rekeyedSchemaKStream = aggregateReKey(aggregateNode, sourceSchemaKStream);

    // Pre aggregate computations
    List<Expression> aggArgExpansionList = new ArrayList<>();
    for (Expression expression: aggregateNode.getRequiredColumnList()) {
      aggArgExpansionList.add(expression);
    }
    for (Expression expression: aggregateNode.getAggregateFunctionArguments()) {
      aggArgExpansionList.add(expression);
    }
    SchemaKStream aggregateArgExpanded = rekeyedSchemaKStream.select(aggArgExpansionList);


    SchemaKGroupedStream schemaKGroupedStream = aggregateArgExpanded.groupByKey(Serdes.String(), genericRowSerde);

    // Aggregate computations
    Map<Integer, KQLAggregateFunction> aggValToAggFunctionMap = new HashMap<>();
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();

    List resultColumns = new ArrayList();
    int nonAggColumnIndex = 0;
    for (Expression expression: aggregateNode.getRequiredColumnList()) {
      String exprStr = expression.toString();
      int index = getIndexInSchema(exprStr, aggregateArgExpanded.getSchema());
      aggValToValColumnMap.put(nonAggColumnIndex, index);
      nonAggColumnIndex++;
      resultColumns.add("");
    }
    int udafIndex = resultColumns.size();
    for (FunctionCall functionCall: aggregateNode.getFunctionList()) {
      KQLFunction aggregateFunctionInfo = KQLFunctions.getAggregateFunction(functionCall.getName()
                                                                            .toString());
      KQLAggregateFunction aggregateFunction = (KQLAggregateFunction) aggregateFunctionInfo
          .getKudfClass().getDeclaredConstructor(Integer.class).newInstance(udafIndex);
      aggValToAggFunctionMap.put(udafIndex, aggregateFunction);
      resultColumns.add(aggregateFunction.getIntialValue());

      udafIndex++;
    }

    SchemaKTable schemaKTable = schemaKGroupedStream.aggregate(
        new KUDAFInitializer(resultColumns),
        new KUDAFAggregator(aggValToAggFunctionMap, aggValToValColumnMap), aggregateNode.getWindowExpression(), genericRowSerde, "KQL_Agg_Query_" + System.currentTimeMillis());

    // Post aggregate computations
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    List<Field> fields = schemaKTable.getSchema().fields();
    for (int i = 0; i < aggregateNode.getRequiredColumnList().size(); i++) {
      schemaBuilder.field(fields.get(i).name(), fields.get(i).schema());
    }
    int aggFunctionVarSuffix = 0;
    for (int i = aggregateNode.getRequiredColumnList().size(); i < fields.size(); i++) {
      Schema fieldSchema;
      String udafName = aggregateNode.getFunctionList().get(aggFunctionVarSuffix).getName()
          .getSuffix();
      KQLFunction aggregateFunction = KQLFunctions.getAggregateFunction(udafName);
      fieldSchema = SchemaUtil.getTypeSchema(aggregateFunction.getReturnType());
      schemaBuilder.field(AggregateExpressionRewriter.AGGREGATE_FUNCTION_VARIABLE_PREFIX
                          + aggFunctionVarSuffix, fieldSchema);
      aggFunctionVarSuffix++;
    }

    Schema aggStageSchema = schemaBuilder.build();

    SchemaKTable finalSchemaKTable = new SchemaKTable(aggStageSchema, schemaKTable.getkTable(),
                                                      schemaKTable.getKeyField(),
                                                      schemaKTable.getSourceSchemaKStreams());

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
                                    .getKqlTopic().getKqlTopicSerDe());

      if (structuredDataSourceNode.getDataSourceType()
          == StructuredDataSource.DataSourceType.KTABLE) {

        KQLTable kqlTable = (KQLTable) structuredDataSourceNode.getStructuredDataSource();
        KTable
            kTable =
            builder
                .table(Serdes.String(), genericRowSerde, kqlTable.getKqlTopic().getKafkaTopicName(),
                       kqlTable.getStateStoreName());
        return new SchemaKTable(sourceNode.getSchema(), kTable,
                                sourceNode.getKeyField(), new ArrayList<>());
      }
      KQLStream kqlStream = (KQLStream) structuredDataSourceNode.getStructuredDataSource();
      KStream
          kStream =
          builder
              .stream(Serdes.String(), genericRowSerde,
                      kqlStream.getKqlTopic().getKafkaTopicName());
      return new SchemaKStream(sourceNode.getSchema(), kStream,
                               sourceNode.getKeyField(), new ArrayList<>());
    }
    throw new KQLException("Unsupported source logical node: " + sourceNode.getClass().getName());
  }

  private SchemaKStream buildJoin(final JoinNode joinNode) throws Exception {
    SchemaKStream leftSchemaKStream = kafkaStreamsDSL(joinNode.getLeft());
    SchemaKStream rightSchemaKStream = kafkaStreamsDSL(joinNode.getRight());
    if (rightSchemaKStream instanceof SchemaKTable) {
      SchemaKTable rightSchemaKTable = (SchemaKTable) rightSchemaKStream;
      if (!leftSchemaKStream.getKeyField().name()
          .equalsIgnoreCase(joinNode.getLeftKeyFieldName())) {
        leftSchemaKStream =
            leftSchemaKStream.selectKey(SchemaUtil.getFieldByName(leftSchemaKStream.getSchema(),
                                                                  joinNode.getLeftKeyFieldName()));
      }
      SchemaKStream joinSchemaKStream;
      switch (joinNode.getType()) {
        case LEFT:
          KQLTopicSerDe joinSerDe = getResultTopicSerde(joinNode);
          String joinKeyFieldName = (joinNode.getLeftAlias() + "." + leftSchemaKStream
              .getKeyField().name()).toUpperCase();
          joinSchemaKStream =
              leftSchemaKStream.leftJoin(rightSchemaKTable, joinNode.getSchema(),
                                         joinNode.getSchema().field(joinKeyFieldName), SerDeUtil.getRowSerDe(joinSerDe));
          break;
        default:
          throw new KQLException("Join type is not supportd yet: " + joinNode.getType());
      }
      return joinSchemaKStream;
    }

    throw new KQLException("Unsupported join logical node: Left: " + joinNode.getLeft() + " , Right: " + joinNode.getRight());
  }

  private KQLTopicSerDe getResultTopicSerde(final PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) node;
      return structuredDataSourceNode.getStructuredDataSource().getKqlTopic().getKqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;
      KQLTopicSerDe leftTopicSerDe = getResultTopicSerde(joinNode.getLeft());
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

  private KQLStructuredDataOutputNode addAvroSchemaToResultTopic(final KQLStructuredDataOutputNode
                                                                     kqlStructuredDataOutputNode,
                                                                 final String avroSchemaFilePath) {
    String avroSchemaFilePathVal = avroSchemaFilePath;
    if (avroSchemaFilePath == null) {
      avroSchemaFilePathVal =
          KQLConfig.DEFAULT_AVRO_SCHEMA_FOLDER_PATH_CONFIG + kqlStructuredDataOutputNode
              .getKqlTopic().getName() + ".avro";
    }
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    String
        avroSchema =
        metastoreUtil.buildAvroSchema(kqlStructuredDataOutputNode.getSchema(),
                                      kqlStructuredDataOutputNode.getKqlTopic().getName());
    metastoreUtil.writeAvroSchemaFile(avroSchema, avroSchemaFilePathVal);
    KQLAvroTopicSerDe kqlAvroTopicSerDe = new KQLAvroTopicSerDe(avroSchemaFilePathVal, avroSchema);
    KQLTopic newKQLTopic = new KQLTopic(kqlStructuredDataOutputNode.getKqlTopic()
                                            .getName(), kqlStructuredDataOutputNode
                                            .getKqlTopic().getKafkaTopicName(), kqlAvroTopicSerDe);

    KQLStructuredDataOutputNode newKQLStructuredDataOutputNode = new KQLStructuredDataOutputNode(
        kqlStructuredDataOutputNode.getId(), kqlStructuredDataOutputNode.getSource(),
        kqlStructuredDataOutputNode.getSchema(), newKQLTopic,
        kqlStructuredDataOutputNode.getKafkaTopicName());
    return newKQLStructuredDataOutputNode;
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
      if (field.name().equalsIgnoreCase(fieldName)) {
        return i;
      }
    }
    return -1;
  }
}
