package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.kql.parser.tree.Expression;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;


public class AggregateNode extends PlanNode {

    private final PlanNode source;
    private final Schema schema;
    private final List<Expression> projectExpressions;
    private final List<Expression> groupByExpressions;

    @JsonCreator
    public AggregateNode(@JsonProperty("id") PlanNodeId id,
                      @JsonProperty("source") PlanNode source,
                      @JsonProperty("schema") Schema schema,
                      @JsonProperty("projectExpressions") List<Expression> projectExpressions,
                      @JsonProperty("groupby") List<Expression> groupByExpressions) {
        super(id);

        this.source = source;
        this.schema = schema;
        this.projectExpressions = projectExpressions;
        this.groupByExpressions = groupByExpressions;
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public Field getKeyField() {
        return null;
    }

    @Override
    public List<PlanNode> getSources() {
        return ImmutableList.of(source);
    }

    public PlanNode getSource() {
        return source;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<Expression> getProjectExpressions() {
        return projectExpressions;
    }
}
