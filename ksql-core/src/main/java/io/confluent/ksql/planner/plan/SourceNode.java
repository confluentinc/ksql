package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.planner.Schema;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public abstract class SourceNode extends PlanNode {

    private final Schema schema;

    public SourceNode(@JsonProperty("id") PlanNodeId id,
                      @JsonProperty("schema") Schema schema)
    {
        super(id);

        requireNonNull(schema, "schema is null");

        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
