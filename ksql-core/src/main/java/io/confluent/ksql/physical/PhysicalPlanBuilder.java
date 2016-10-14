package io.confluent.ksql.physical;


import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.planner.Schema;
import io.confluent.ksql.planner.plan.*;
import io.confluent.ksql.serde.JsonPOJODeserializer;
import io.confluent.ksql.serde.JsonPOJOSerializer;
import io.confluent.ksql.structured.SchemaStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PhysicalPlanBuilder {

    static Serde<GenericRow> genericRowSerde = null;
    KStreamBuilder builder;

    public PhysicalPlanBuilder(KStreamBuilder builder) {

        this.builder = builder;
    }

    public SchemaStream buildPhysicalPlan(PlanNode logicalPlanRoot) throws Exception {
        SchemaStream resultSchemaStream = resultsSchemaStream((OutputNode)logicalPlanRoot);
//        resultSchemaStream.getkStream().
        return resultSchemaStream;
    }

    private SchemaStream resultsSchemaStream(OutputNode outputNode) throws Exception {
        SchemaStream projectedSchemaStream = projectedSchemaStream((ProjectNode)outputNode.getSource());
        if(outputNode instanceof OutputKafkaTopicNode) {
            OutputKafkaTopicNode outputKafkaTopicNode = (OutputKafkaTopicNode)outputNode;
            SchemaStream resultSchemaStream = projectedSchemaStream.into(outputKafkaTopicNode.getKafkaTopicName());
            return resultSchemaStream;
        }
        return null;
    }

    private SchemaStream projectedSchemaStream(ProjectNode projectNode) throws Exception {
        SchemaStream filteredSchemaStream = buildFilter((FilterNode) projectNode.getSource());
//        SchemaStream projectedSchemaStream = filteredSchemaStream.select(projectNode.getSchema());
        SchemaStream projectedSchemaStream = filteredSchemaStream.select(projectNode.getProjectExpressions(), projectNode.getSchema());
        return projectedSchemaStream;
    }


    private SchemaStream buildFilter(FilterNode filterNode) throws Exception {
        SchemaStream sourceSchemaStream = buildSource((SourceNode)filterNode.getSource());
        SchemaStream filteredSchemaStream = sourceSchemaStream.filter(filterNode.getPredicate());
        return filteredSchemaStream;
    }

    private  SchemaStream buildSource(SourceNode sourceNode) {
        if(sourceNode instanceof SourceKafkaTopicNode) {
            SourceKafkaTopicNode sourceKafkaTopicNode = (SourceKafkaTopicNode) sourceNode;
            KStream kStream = builder.stream(Serdes.String(), getGenericRowSerde(), sourceKafkaTopicNode.getTopicName());
            return new SchemaStream(sourceKafkaTopicNode.getSchema(), kStream);
        }
        return null;
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
}
