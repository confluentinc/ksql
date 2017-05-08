/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.kql.parser.tree.Expression;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class ProjectNode
    extends PlanNode {

  private final PlanNode source;
  private final Schema schema;
  private final Field keyField;
  private final List<Expression> projectExpressions;

  // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
  @JsonCreator
  public ProjectNode(@JsonProperty("id") final PlanNodeId id,
                     @JsonProperty("source") final PlanNode source,
                     @JsonProperty("schema") final Schema schema,
                     @JsonProperty("projectExpressions") final List<Expression> projectExpressions) {
    super(id);

    requireNonNull(source, "source is null");
    requireNonNull(schema, "schema is null");
    requireNonNull(projectExpressions, "projectExpressions is null");

    this.source = source;
    this.schema = schema;
    this.keyField = source.getKeyField();
    this.projectExpressions = projectExpressions;
  }


  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @JsonProperty
  public PlanNode getSource() {
    return source;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Field getKeyField() {
    return keyField;
  }

  public List<Expression> getProjectExpressions() {
    return projectExpressions;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitProject(this, context);
  }
}
