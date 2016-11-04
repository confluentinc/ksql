package io.confluent.ksql.physical;


import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.serde.JsonPOJODeserializer;
import io.confluent.ksql.serde.JsonPOJOSerializer;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.structured.SchemaStream;
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
    OutputKafkaTopicNode planSink = null;

    public PhysicalPlanBuilder(KStreamBuilder builder) {

        this.builder = builder;
    }

    public SchemaStream buildPhysicalPlan(PlanNode logicalPlanRoot) throws Exception {
        return kafkaStreamsDSL(logicalPlanRoot);
    }

    private SchemaStream kafkaStreamsDSL(PlanNode planNode) throws Exception {
        if(planNode instanceof SourceNode) {
            return buildSource((SourceNode) planNode);
        } else if(planNode instanceof JoinNode) {
            return buildJoin((JoinNode) planNode);
        } else if (planNode instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) planNode;
            SchemaStream projectedSchemaStream = buildProject(projectNode);
            return projectedSchemaStream;
        } else if (planNode instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) planNode;
            SchemaStream filteredSchemaStream = buildFilter(filterNode);
            return filteredSchemaStream;
        } else if (planNode instanceof OutputNode) {
            OutputNode outputNode = (OutputNode) planNode;
            SchemaStream outputSchemaStream = buildOutput(outputNode);
            return outputSchemaStream;
        }
        throw new KSQLException("Unsupported logical plan node: "+planNode.getId()+" , Type: "+planNode.getClass().getName());
    }

    private SchemaStream buildOutput(OutputNode outputNode) throws Exception {

        if(outputNode instanceof OutputKafkaTopicNode) {
            OutputKafkaTopicNode outputKafkaTopicNode = (OutputKafkaTopicNode)outputNode;
            SchemaStream schemaStream = kafkaStreamsDSL(outputNode.getSource());
            SchemaStream resultSchemaStream = schemaStream.into(outputKafkaTopicNode.getKafkaTopicName());
            this.planSink = outputKafkaTopicNode;
            return resultSchemaStream;
        }
        throw new KSQLException("Unsupported output logical node: "+outputNode.getClass().getName());
    }

    private SchemaStream buildProject(ProjectNode projectNode) throws Exception {
//        SchemaStream schemaStream = kafkaStreamsDSL(projectNode.getSource());
        SchemaStream projectedSchemaStream = kafkaStreamsDSL(projectNode.getSource()).select(projectNode.getProjectExpressions(), projectNode.getSchema());
        return projectedSchemaStream;
    }


    private SchemaStream buildFilter(FilterNode filterNode) throws Exception {
//        SchemaStream schemaStream = kafkaStreamsDSL(filterNode.getSource());
        SchemaStream filteredSchemaStream = kafkaStreamsDSL(filterNode.getSource()).filter(filterNode.getPredicate());
        return filteredSchemaStream;
    }

    private SchemaStream buildSource(SourceNode sourceNode) {
        if(sourceNode instanceof SourceKafkaTopicNode) {
            SourceKafkaTopicNode sourceKafkaTopicNode = (SourceKafkaTopicNode) sourceNode;
            if (sourceKafkaTopicNode.getDataSourceType() == DataSource.DataSourceType.KTABLE) {
                KTable kTable = builder.table(Serdes.String(), getGenericRowSerde(), sourceKafkaTopicNode.getTopicName(), sourceKafkaTopicNode.getTopicName()+"_store");
                return new SchemaKTable(sourceKafkaTopicNode.getSchema(), kTable, sourceKafkaTopicNode.getKeyField());
            }
            KStream kStream = builder.stream(Serdes.String(), getGenericRowSerde(), sourceKafkaTopicNode.getTopicName());
            return new SchemaStream(sourceKafkaTopicNode.getSchema(), kStream, sourceKafkaTopicNode.getKeyField());
        }
        throw new KSQLException("Unsupported source logical node: "+sourceNode.getClass().getName());
    }

    private SchemaStream buildJoin(JoinNode joinNode) throws Exception {
        SchemaStream leftSchemaStream = kafkaStreamsDSL(joinNode.getLeft());
        SchemaStream rightSchemaStream = kafkaStreamsDSL(joinNode.getRight());
        if(rightSchemaStream instanceof SchemaKTable) {
            SchemaKTable rightSchemaKTable = (SchemaKTable)rightSchemaStream;
            if (!leftSchemaStream.getKeyField().name().equalsIgnoreCase(joinNode.getLeftKeyFieldName())) {
                leftSchemaStream = leftSchemaStream.selectKey(SchemaUtil.getFieldByName(leftSchemaStream.getSchema(), joinNode.getLeftKeyFieldName()));
            }
            SchemaStream joinSchemaStream;// = leftSchemaStream.leftJoin(rightSchemaKTable, joinNode.getSchema(), joinNode.getKeyField());
            switch (joinNode.getType()) {
                case LEFT:
                    joinSchemaStream = leftSchemaStream.leftJoin(rightSchemaKTable, joinNode.getSchema(), joinNode.getSchema().field(joinNode.getLeftAlias()+"."+leftSchemaStream.getKeyField().name()));
                    break;
                default:
                    throw new KSQLException("Join type is not supportd yet: "+joinNode.getType());
            }
            return joinSchemaStream;
        }

        throw new KSQLException("Unsupported join logical node: Left: "+joinNode.getLeft()+" , Right: "+joinNode.getRight());
    }

    public static Serde<GenericRow> getGenericRowSerde() {
        if(genericRowSerde == null) {
            Map<String, Object> serdeProps = new HashMap<>();

            final Serializer<GenericRow> genericRowSerializer = new JsonPOJOSerializer<>();
            serdeProps.put("JsonPOJOClass", GenericRow.class);
            genericRowSerializer.configure(serdeProps, false);

            final Deserializer<GenericRow> genericRowDeserializer = new JsonPOJODeserializer<>();
            serdeProps.put("JsonPOJOClass", GenericRow.class);
            genericRowDeserializer.configure(serdeProps, false);

            genericRowSerde = Serdes.serdeFrom(genericRowSerializer,  genericRowDeserializer);
        }
        return genericRowSerde;
    }

    public KStreamBuilder getBuilder() {
        return builder;
    }

    public OutputKafkaTopicNode getPlanSink() {
        return planSink;
    }
}
