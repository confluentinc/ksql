/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableList;

import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.WindowExpression;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;


public class AggregateNode extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final List<Expression> projectExpressions;
  private final List<Expression> groupByExpressions;
  private final WindowExpression windowExpression;

  @JsonCreator
  public AggregateNode(@JsonProperty("id") final PlanNodeId id,
                       @JsonProperty("source") final PlanNode source,
                       @JsonProperty("schema") final Schema schema,
                       @JsonProperty("projectExpressions") final List<Expression> projectExpressions,
                       @JsonProperty("groupby") final List<Expression> groupByExpressions,
                       @JsonProperty("window") final WindowExpression windowExpression) {
    super(id);

    this.source = source;
    this.schema = schema;
    this.projectExpressions = projectExpressions;
    this.groupByExpressions = groupByExpressions;
    this.windowExpression = windowExpression;
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

  public WindowExpression getWindowExpression() {
    return windowExpression;
  }
}
