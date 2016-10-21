package io.confluent.ksql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.data.Schema;

import static java.util.Objects.requireNonNull;

public class OutputKafkaTopicNode extends OutputNode {

    final String kafkaTopicName;
    @JsonCreator
    public OutputKafkaTopicNode(@JsonProperty("id") PlanNodeId id,
                      @JsonProperty("source") PlanNode source,
                      @JsonProperty("schema")Schema schema,
                      @JsonProperty("topicName")String topicName )
    {
        super(id, source, schema);
        this.kafkaTopicName = topicName;

    }

    public String getKafkaTopicName() {
        return kafkaTopicName;
    }
}
