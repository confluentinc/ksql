package io.confluent.kql.physical;


import io.confluent.kql.function.udaf.sum.Sum_KUDAF;
import io.confluent.kql.function.udf.KUDF;
import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetastoreUtil;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.planner.plan.*;
import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.structured.SchemaKGroupedStream;
import io.confluent.kql.structured.SchemaKTable;
import io.confluent.kql.structured.SchemaKStream;
import io.confluent.kql.util.*;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class PhysicalPlanBuilder {

  KStreamBuilder builder;
  OutputNode planSink = null;

  public PhysicalPlanBuilder(KStreamBuilder builder) {

    this.builder = builder;
  }

  public SchemaKStream buildPhysicalPlan(PlanNode logicalPlanRoot) throws Exception {
    return kafkaStreamsDSL(logicalPlanRoot);
  }

  private SchemaKStream kafkaStreamsDSL(PlanNode planNode) throws Exception {
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

  private SchemaKStream buildOutput(OutputNode outputNode) throws Exception {
    SchemaKStream schemaKStream = kafkaStreamsDSL(outputNode.getSource());
    if (outputNode instanceof KQLStructuredDataOutputNode) {
      KQLStructuredDataOutputNode kqlStructuredDataOutputNode = (KQLStructuredDataOutputNode)
          outputNode;
      if (kqlStructuredDataOutputNode.getKqlTopic().getKqlTopicSerDe() instanceof KQLAvroTopicSerDe) {
        KQLAvroTopicSerDe kqlAvroTopicSerDe = (KQLAvroTopicSerDe) kqlStructuredDataOutputNode
            .getKqlTopic().getKqlTopicSerDe();
        kqlStructuredDataOutputNode = addAvroSchemaToResultTopic(kqlStructuredDataOutputNode,
                                                                 kqlAvroTopicSerDe.getSchemaFilePath());
      }
      SchemaKStream resultSchemaStream = schemaKStream.into(kqlStructuredDataOutputNode
                                                                .getKafkaTopicName(),
                                                            SerDeUtil.getRowSerDe
                                                                (kqlStructuredDataOutputNode
                                                                     .getKqlTopic().getKqlTopicSerDe()));
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

  private SchemaKStream buildAggregate(AggregateNode aggregateNode) throws Exception {

      StructuredDataSourceNode sourceNode = (StructuredDataSourceNode) aggregateNode.getSource();
      Serde<GenericRow> genericRowSerde = SerDeUtil.getRowSerDe(sourceNode.getStructuredDataSource()
              .getKQLTopic().getKqlTopicSerDe());
      SchemaKStream sourceSchemaKStream = kafkaStreamsDSL(aggregateNode.getSource());

      SchemaKStream rekeyedSchemaKStream = aggregateReKey(aggregateNode, sourceSchemaKStream);
      return rekeyedSchemaKStream;
//      SchemaKGroupedStream schemaKGroupedStream = sourceSchemaKStream.groupByKey(Serdes.String(), genericRowSerde);
//
//
//      int aggColumnIndexInResult = -1;
//      Object aggColumnInitialValueInResult = 0.0;
//      Map<Integer, Integer> resultToSourceColumnMap_agg = new HashMap<>();
//      Map<Integer, Integer> resultToSourceColumnMap_nonAgg = new HashMap<>();
//
//      List resultColumns = new ArrayList();
//      for (int i = 0; i < aggregateNode.getProjectExpressions().size(); i++) {
//          Expression expression = aggregateNode.getProjectExpressions().get(i);
//          if (expression instanceof FunctionCall) {
//              FunctionCall functionCall = (FunctionCall)expression;
//              String argStr = functionCall.getArguments().get(0).toString();
//              int index = getIndexInSchema(argStr, aggregateNode.getSource().getSchema());
//              aggColumnIndexInResult = i;
//              resultToSourceColumnMap_agg.put(i, index);
//          } else {
//              String exprStr = expression.toString();
//              int index = getIndexInSchema(exprStr, aggregateNode.getSource().getSchema());
//              resultToSourceColumnMap_nonAgg.put(i, index);
//          }
//          resultColumns.add("");
//      }
//      GenericRow resultGenericRow = new GenericRow(resultColumns);
//      System.out.print("");
//      Sum_KUDAF sum_kudaf = new Sum_KUDAF(resultGenericRow, aggColumnIndexInResult, aggColumnInitialValueInResult, resultToSourceColumnMap_agg, resultToSourceColumnMap_nonAgg);
//
//
//      SchemaKStream schemaKStream = schemaKGroupedStream.aggregate(sum_kudaf.getInitializer(), sum_kudaf.getAggregator(), genericRowSerde, "Testsing"+System.currentTimeMillis());
//      return schemaKStream;
  }

  private SchemaKStream buildProject(ProjectNode projectNode) throws Exception {
    SchemaKStream
        projectedSchemaStream =
        kafkaStreamsDSL(projectNode.getSource())
            .select(projectNode.getProjectExpressions(), projectNode.getSchema());
    return projectedSchemaStream;
  }


  private SchemaKStream buildFilter(FilterNode filterNode) throws Exception {
    SchemaKStream
        filteredSchemaKStream =
        kafkaStreamsDSL(filterNode.getSource()).filter(filterNode.getPredicate());
    return filteredSchemaKStream;
  }

  private SchemaKStream buildSource(SourceNode sourceNode) {
    if (sourceNode instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) sourceNode;

      Serde<GenericRow> genericRowSerde = SerDeUtil.getRowSerDe(structuredDataSourceNode.getStructuredDataSource()
                                                                    .getKQLTopic().getKqlTopicSerDe());

      if (structuredDataSourceNode.getDataSourceType() == StructuredDataSource.DataSourceType.KTABLE) {

        KQLTable kqlTable = (KQLTable) structuredDataSourceNode.getStructuredDataSource();
        KTable
            kTable =
            builder
                .table(Serdes.String(), genericRowSerde, kqlTable.getKQLTopic().getKafkaTopicName(),
                       kqlTable.getStateStoreName());
        return new SchemaKTable(sourceNode.getSchema(), kTable,
                                sourceNode.getKeyField(), new ArrayList<>());
      }
      KQLStream kqlStream = (KQLStream) structuredDataSourceNode.getStructuredDataSource();
      KStream
          kStream =
          builder
              .stream(Serdes.String(), genericRowSerde, kqlStream.getKQLTopic().getKafkaTopicName());
      return new SchemaKStream(sourceNode.getSchema(), kStream,
                               sourceNode.getKeyField(), new ArrayList<>());
    }
    throw new KQLException("Unsupported source logical node: " + sourceNode.getClass().getName());
  }

  private SchemaKStream buildJoin(JoinNode joinNode) throws Exception {
    SchemaKStream leftSchemaKStream = kafkaStreamsDSL(joinNode.getLeft());
    SchemaKStream rightSchemaKStream = kafkaStreamsDSL(joinNode.getRight());
    if (rightSchemaKStream instanceof SchemaKTable) {
      SchemaKTable rightSchemaKTable = (SchemaKTable) rightSchemaKStream;
      if (!leftSchemaKStream.getKeyField().name().equalsIgnoreCase(joinNode.getLeftKeyFieldName())) {
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
                                        joinNode.getSchema().field(
                                            joinKeyFieldName), SerDeUtil.getRowSerDe
                      (joinSerDe));
          break;
        default:
          throw new KQLException("Join type is not supportd yet: " + joinNode.getType());
      }
      return joinSchemaKStream;
    }

    throw new KQLException(
        "Unsupported join logical node: Left: " + joinNode.getLeft() + " , Right: " + joinNode
            .getRight());
  }

  private KQLTopicSerDe getResultTopicSerde(PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode)node;
      return structuredDataSourceNode.getStructuredDataSource().getKQLTopic().getKqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;
      KQLTopicSerDe leftTopicSerDe = getResultTopicSerde(joinNode.getLeft());
      return leftTopicSerDe;
    } else return getResultTopicSerde(node.getSources().get(0));
  }



  public KStreamBuilder getBuilder() {
    return builder;
  }

  public OutputNode getPlanSink() {
    return planSink;
  }

  private KQLStructuredDataOutputNode addAvroSchemaToResultTopic(KQLStructuredDataOutputNode
                                                                     kqlStructuredDataOutputNode,
                                                                 String avroSchemaFilePath) {

    if (avroSchemaFilePath == null) {
      avroSchemaFilePath = KQLConfig.DEFAULT_AVRO_SCHEMA_FOLDER_PATH_CONFIG +kqlStructuredDataOutputNode.getKqlTopic().getName()+".avro";
    }
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    String avroSchema = metastoreUtil.buildAvroSchema(kqlStructuredDataOutputNode.getSchema(), kqlStructuredDataOutputNode.getKqlTopic().getName());
    metastoreUtil.writeAvroSchemaFile(avroSchema,avroSchemaFilePath);
    KQLAvroTopicSerDe kqlAvroTopicSerDe = new KQLAvroTopicSerDe(avroSchemaFilePath, avroSchema);
    KQLTopic newKQLTopic = new KQLTopic(kqlStructuredDataOutputNode.getKqlTopic()
                                            .getName(), kqlStructuredDataOutputNode
                                            .getKqlTopic().getKafkaTopicName(), kqlAvroTopicSerDe);

    KQLStructuredDataOutputNode newKQLStructuredDataOutputNode = new KQLStructuredDataOutputNode
        (kqlStructuredDataOutputNode.getId(), kqlStructuredDataOutputNode.getSource(),
         kqlStructuredDataOutputNode.getSchema(), newKQLTopic, kqlStructuredDataOutputNode.getKafkaTopicName());
    return newKQLStructuredDataOutputNode;
  }

  private SchemaKStream aggregateReKey(AggregateNode aggregateNode, SchemaKStream sourceSchemaKStream) {
      String aggregateKeyName = "";
      List<Integer> newKeyIndexes = new ArrayList<>();
      boolean addSeparator = false;
      for (Expression groupByExpr: aggregateNode.getGroupByExpressions()) {
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

      return new SchemaKStream(sourceSchemaKStream.getSchema(), rekeyedKStream, newKeyField, Arrays.asList(sourceSchemaKStream));
  }

    private int getIndexInSchema(String fieldName, Schema schema) {
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
