package io.confluent.kql.physical;


import io.confluent.kql.metastore.KQLStream;
import io.confluent.kql.metastore.KQLTable;
import io.confluent.kql.metastore.KQLTopic;
import io.confluent.kql.metastore.MetastoreUtil;
import io.confluent.kql.metastore.StructuredDataSource;
import io.confluent.kql.planner.plan.*;
import io.confluent.kql.serde.KQLTopicSerDe;
import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.structured.SchemaKTable;
import io.confluent.kql.structured.SchemaKStream;
import io.confluent.kql.util.KQLConfig;
import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.SchemaUtil;
import io.confluent.kql.util.SerDeUtil;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

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
      KQLConsoleOutputNode KQLConsoleOutputNode = (KQLConsoleOutputNode) outputNode;
      this.planSink = KQLConsoleOutputNode;
      return resultSchemaStream;
    }
    throw new KQLException("Unsupported output logical node: " + outputNode.getClass().getName());
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
                                sourceNode.getKeyField());
      }
      KQLStream kqlStream = (KQLStream) structuredDataSourceNode.getStructuredDataSource();
      KStream
          kStream =
          builder
              .stream(Serdes.String(), genericRowSerde, kqlStream.getKQLTopic().getKafkaTopicName());
      return new SchemaKStream(sourceNode.getSchema(), kStream,
                               sourceNode.getKeyField());
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
      avroSchemaFilePath = KQLConfig.DEFAULT_AVRO_SCHEMA_FOLDER_PATH_CONFIG+kqlStructuredDataOutputNode.getKqlTopic().getName()+".avro";
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
}
