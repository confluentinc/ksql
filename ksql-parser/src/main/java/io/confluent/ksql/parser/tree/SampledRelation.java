/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

  public SampledRelation(
      final Relation relation,
      final Type type,
      final Expression samplePercentage,
      final boolean rescaled,
      final Optional<List<Expression>> columnsToStratifyOn) {
    this(Optional.empty(), relation, type, samplePercentage, rescaled, columnsToStratifyOn);
  }

  public SampledRelation(
      final NodeLocation location,
      final Relation relation,
      final Type type,
      final Expression samplePercentage,
      final boolean rescaled,
      final Optional<List<Expression>> columnsToStratifyOn) {
    this(Optional.of(location), relation, type, samplePercentage, rescaled, columnsToStratifyOn);
  }

  private SampledRelation(
      final Optional<NodeLocation> location,
      final Relation relation,
      final Type type,
      final Expression samplePercentage,
      final boolean rescaled,
      final Optional<List<Expression>> columnsToStratifyOn) {
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
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SampledRelation that = (SampledRelation) o;
    return Objects.equals(relation, that.relation)
           && Objects.equals(type, that.type)
           && Objects.equals(samplePercentage, that.samplePercentage)
           && Objects.equals(columnsToStratifyOn, that.columnsToStratifyOn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, type, samplePercentage, columnsToStratifyOn);
  }
}
