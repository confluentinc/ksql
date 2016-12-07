package io.confluent.ksql.physical;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.serde.json.KQLJsonPOJODeserializer;
import io.confluent.ksql.serde.json.KQLJsonPOJOSerializer;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.SchemaUtil;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.HashMap;
import java.util.Map;

public class PhysicalPlanBuilder {

  static Serde<GenericRow> genericRowSerde = null;
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
    if (outputNode instanceof OutputKafkaTopicNode) {
      OutputKafkaTopicNode outputKafkaTopicNode = (OutputKafkaTopicNode) outputNode;
      SchemaKStream resultSchemaStream = schemaKStream.into(outputKafkaTopicNode.getKafkaTopicName());
      this.planSink = outputKafkaTopicNode;
      return resultSchemaStream;
    } else if (outputNode instanceof OutputKSQLConsoleNode) {
      SchemaKStream resultSchemaStream = schemaKStream.print();
      OutputKSQLConsoleNode outputKSQLConsoleNode = (OutputKSQLConsoleNode) outputNode;
      this.planSink = outputKSQLConsoleNode;
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
    if (sourceNode instanceof SourceKafkaTopicNode) {
      SourceKafkaTopicNode sourceKafkaTopicNode = (SourceKafkaTopicNode) sourceNode;
      if (sourceKafkaTopicNode.getDataSourceType() == DataSource.DataSourceType.KTABLE) {
        KTable
            kTable =
            builder
                .table(Serdes.String(), getGenericRowSerde(), sourceKafkaTopicNode.getTopicName(),
                       sourceKafkaTopicNode.getTopicName() + "_store");
        return new SchemaKTable(sourceKafkaTopicNode.getSchema(), kTable,
                                sourceKafkaTopicNode.getKeyField());
      }
      KStream
          kStream =
          builder
              .stream(Serdes.String(), getGenericRowSerde(), sourceKafkaTopicNode.getTopicName());
      return new SchemaKStream(sourceKafkaTopicNode.getSchema(), kStream,
                               sourceKafkaTopicNode.getKeyField());
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
          joinSchemaKStream =
              leftSchemaKStream.leftJoin(rightSchemaKTable, joinNode.getSchema(),
                                        joinNode.getSchema().field(
                                            joinNode.getLeftAlias() + "." + leftSchemaKStream
                                                .getKeyField().name()));
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

  public static Serde<GenericRow> getGenericRowSerde() {
    if (genericRowSerde == null) {
      Map<String, Object> serdeProps = new HashMap<>();

      final Serializer<GenericRow> genericRowSerializer = new KQLJsonPOJOSerializer<>();
      serdeProps.put("JsonPOJOClass", GenericRow.class);
      genericRowSerializer.configure(serdeProps, false);

      final Deserializer<GenericRow> genericRowDeserializer = new KQLJsonPOJODeserializer<>();
      serdeProps.put("JsonPOJOClass", GenericRow.class);
      genericRowDeserializer.configure(serdeProps, false);

      genericRowSerde = Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
    }
    return genericRowSerde;
  }

  public KStreamBuilder getBuilder() {
    return builder;
  }

  public OutputNode getPlanSink() {
    return planSink;
  }
}
