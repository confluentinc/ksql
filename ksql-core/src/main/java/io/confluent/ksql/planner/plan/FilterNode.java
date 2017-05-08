/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Expression;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class FilterNode
    extends PlanNode {

  private final PlanNode source;
  private final Expression predicate;
  private final Schema schema;
  private final Field keyField;

  @JsonCreator
  public FilterNode(@JsonProperty("id") final PlanNodeId id,
                    @JsonProperty("source") final PlanNode source,
                    @JsonProperty("predicate") final Expression predicate) {
    super(id);

    this.source = source;
    this.schema = source.getSchema();
    this.predicate = predicate;
    this.keyField = source.getKeyField();
  }

  @JsonProperty("predicate")
  public Expression getPredicate() {
    return predicate;
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public Field getKeyField() {
    return keyField;
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @JsonProperty("source")
  public PlanNode getSource() {
    return source;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitFilter(this, context);
  }
}
