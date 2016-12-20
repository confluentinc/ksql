package io.confluent.ksql.physical;


import io.confluent.ksql.metastore.KQLStream;
import io.confluent.ksql.metastore.KQLTable;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.serde.KQLTopicSerDe;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;

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
    throw new KSQLException(
        "Unsupported logical plan node: " + planNode.getId() + " , Type: " + planNode.getClass()
            .getName());
  }

  private SchemaKStream buildOutput(OutputNode outputNode) throws Exception {
    SchemaKStream schemaKStream = kafkaStreamsDSL(outputNode.getSource());
    if (outputNode instanceof KQLStructuredDataOutputNode) {
      KQLStructuredDataOutputNode kqlStructuredDataOutputNode = (KQLStructuredDataOutputNode)
          outputNode;
      KQLTopicSerDe topicSerDe = getResultTopicSerde(kqlStructuredDataOutputNode);

//      SchemaKStream resultSchemaStream = schemaKStream.into(KQLStructuredDataOutputNode
//                                                                .getKafkaTopicName(), SerDeUtil.getRowSerDe
//          (topicSerDe));
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
    throw new KSQLException("Unsupported output logical node: " + outputNode.getClass().getName());
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
    throw new KSQLException("Unsupported source logical node: " + sourceNode.getClass().getName());
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
          joinSchemaKStream =
              leftSchemaKStream.leftJoin(rightSchemaKTable, joinNode.getSchema(),
                                        joinNode.getSchema().field(
                                            joinNode.getLeftAlias() + "." + leftSchemaKStream
                                                .getKeyField().name()), SerDeUtil.getRowSerDe
                      (joinSerDe));
          break;
        default:
          throw new KSQLException("Join type is not supportd yet: " + joinNode.getType());
      }
      return joinSchemaKStream;
    }

    throw new KSQLException(
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
      // Keep the left as defult!
//      KQLTopicSerDe rightTopicSerDe = getResultTopicSerde(joinNode.getRight());
      return leftTopicSerDe;
    } else return getResultTopicSerde(node.getSources().get(0));
  }



  public KStreamBuilder getBuilder() {
    return builder;
  }

  public OutputNode getPlanSink() {
    return planSink;
  }
}
