/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.WindowExpression;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.List;


public class AggregateNode extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final List<Expression> projectExpressions;
  private final List<Expression> groupByExpressions;
  private final WindowExpression windowExpression;
  private final List<Expression> aggregateFunctionArguments;

  private final List<FunctionCall> functionList;
  private final List<Expression> requiredColumnList;

  private final List<Expression> nonAggResultColumns;

  private final List<Expression> finalSelectExpressions;

  private final Expression havingExpressions;

  @JsonCreator
  public AggregateNode(@JsonProperty("id") final PlanNodeId id,
                       @JsonProperty("source") final PlanNode source,
                       @JsonProperty("schema") final Schema schema,
                       @JsonProperty("projectExpressions")
                         final List<Expression> projectExpressions,
                       @JsonProperty("groupby") final List<Expression> groupByExpressions,
                       @JsonProperty("window") final WindowExpression windowExpression,
                       @JsonProperty("aggregateFunctionArguments")
                         final List<Expression> aggregateFunctionArguments,
                       @JsonProperty("functionList") final List<FunctionCall> functionList,
                       @JsonProperty("requiredColumnList") final List<Expression>
                             requiredColumnList,
                       @JsonProperty("nonAggResultColumns") final List<Expression>
                             nonAggResultColumns,
                       @JsonProperty("finalSelectExpressions") final List<Expression>
                             finalSelectExpressions,
                       @JsonProperty("havingExpressions") final Expression
                             havingExpressions) {
    super(id);

    this.source = source;
    this.schema = schema;
    this.projectExpressions = projectExpressions;
    this.groupByExpressions = groupByExpressions;
    this.windowExpression = windowExpression;
    this.aggregateFunctionArguments = aggregateFunctionArguments;
    this.functionList = functionList;
    this.requiredColumnList = requiredColumnList;
    this.nonAggResultColumns = nonAggResultColumns;
    this.finalSelectExpressions = finalSelectExpressions;
    this.havingExpressions = havingExpressions;
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

  public List<Expression> getAggregateFunctionArguments() {
    return aggregateFunctionArguments;
  }

  public List<FunctionCall> getFunctionList() {
    return functionList;
  }

  public List<Expression> getRequiredColumnList() {
    return requiredColumnList;
  }

  public List<Expression> getNonAggResultColumns() {
    return nonAggResultColumns;
  }

  public List<Expression> getFinalSelectExpressions() {
    return finalSelectExpressions;
  }

  public Expression getHavingExpressions() {
    return havingExpressions;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitAggregate(this, context);
  }
}
