package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.planner.KSQLSchema;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SourceNode extends PlanNode {

    private final KSQLSchema schema;

    public SourceNode(@JsonProperty("id") PlanNodeId id,
                      @JsonProperty("schema") KSQLSchema schema)
    {
        super(id);

        requireNonNull(schema, "schema is null");

        this.schema = schema;
    }

    @Override
    public KSQLSchema getSchema() {
        return schema;
    }
}
