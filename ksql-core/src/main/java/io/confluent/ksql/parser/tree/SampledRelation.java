/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SampledRelation
    extends Relation {

  public enum Type {
    BERNOULLI,
    POISSONIZED,
    SYSTEM
  }

  private final Relation relation;
  private final Type type;
  private final Expression samplePercentage;
  private final boolean rescaled;
  private final Optional<List<Expression>> columnsToStratifyOn;

  public SampledRelation(Relation relation, Type type, Expression samplePercentage,
                         boolean rescaled, Optional<List<Expression>> columnsToStratifyOn) {
    this(Optional.empty(), relation, type, samplePercentage, rescaled, columnsToStratifyOn);
  }

  public SampledRelation(NodeLocation location, Relation relation, Type type,
                         Expression samplePercentage, boolean rescaled,
                         Optional<List<Expression>> columnsToStratifyOn) {
    this(Optional.of(location), relation, type, samplePercentage, rescaled, columnsToStratifyOn);
  }

  private SampledRelation(Optional<NodeLocation> location, Relation relation, Type type,
                          Expression samplePercentage, boolean rescaled,
                          Optional<List<Expression>> columnsToStratifyOn) {
    super(location);
    this.relation = requireNonNull(relation, "relation is null");
    this.type = requireNonNull(type, "type is null");
    this.samplePercentage = requireNonNull(samplePercentage, "samplePercentage is null");
    this.rescaled = rescaled;

    if (columnsToStratifyOn.isPresent()) {
      this.columnsToStratifyOn =
          Optional.<List<Expression>>of(ImmutableList.copyOf(columnsToStratifyOn.get()));
    } else {
      this.columnsToStratifyOn = columnsToStratifyOn;
    }
  }

  public Relation getRelation() {
    return relation;
  }

  public Type getType() {
    return type;
  }

  public Expression getSamplePercentage() {
    return samplePercentage;
  }

  public boolean isRescaled() {
    return rescaled;
  }

  public Optional<List<Expression>> getColumnsToStratifyOn() {
    return columnsToStratifyOn;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSampledRelation(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("relation", relation)
        .add("type", type)
        .add("samplePercentage", samplePercentage)
        .add("columnsToStratifyOn", columnsToStratifyOn)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SampledRelation that = (SampledRelation) o;
    return Objects.equals(relation, that.relation) &&
           Objects.equals(type, that.type) &&
           Objects.equals(samplePercentage, that.samplePercentage) &&
           Objects.equals(columnsToStratifyOn, that.columnsToStratifyOn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, type, samplePercentage, columnsToStratifyOn);
  }
}
