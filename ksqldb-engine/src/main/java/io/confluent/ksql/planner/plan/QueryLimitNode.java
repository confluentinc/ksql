package io.confluent.ksql.planner.plan;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.OptionalInt;

public class QueryLimitNode extends SingleSourcePlanNode {
    private final int limit;

    public QueryLimitNode(
            final PlanNodeId id,
            final PlanNode source,
            final OptionalInt limit
            ) {
        super(id, source.getNodeOutputType(), source.getSourceName(), source);
        this.limit = limit.getAsInt();
    }

    @Override
    public LogicalSchema getSchema() {
        return getSource().getSchema();
    }

    @Override
    public SchemaKStream<?> buildStream(final PlanBuildContext buildCtx) {
        throw new UnsupportedOperationException();
    }

    public int getLimit() {return limit;}
}
