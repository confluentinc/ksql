/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.windows;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class HoppingWindowExpression extends KsqlWindowExpression {

  private final WindowTimeClause size;
  private final WindowTimeClause advanceBy;

  @JsonCreator
  public static HoppingWindowExpression of(
      @JsonProperty(value = "size", required = true) final WindowTimeClause size,
      @JsonProperty(value = "advanceBy", required = true) final WindowTimeClause advanceBy,
      @JsonProperty(value = "retention") final WindowTimeClause retention,
      @JsonProperty(value = "gracePeriod") final WindowTimeClause gracePeriod,
      @JsonProperty(value = "emitStrategy") final OutputRefinement emitStrategy
  ) {
    return new HoppingWindowExpression(
        Optional.empty(),
        size,
        advanceBy,
        Optional.ofNullable(retention),
        Optional.ofNullable(gracePeriod),
        Optional.ofNullable(emitStrategy)
    );
  }

  public HoppingWindowExpression(
      final WindowTimeClause size,
      final WindowTimeClause advanceBy
  ) {
    this(
        Optional.empty(),
        size,
        advanceBy,
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );
  }

  public HoppingWindowExpression(
      final Optional<NodeLocation> location,
      final WindowTimeClause size,
      final WindowTimeClause advanceBy,
      final Optional<WindowTimeClause> retention,
      final Optional<WindowTimeClause> gracePeriod
  ) {
    this(location, size, advanceBy, retention, gracePeriod, Optional.empty());
  }

  public HoppingWindowExpression(
      final Optional<NodeLocation> location,
      final WindowTimeClause size,
      final WindowTimeClause advanceBy,
      final Optional<WindowTimeClause> retention,
      final Optional<WindowTimeClause> gracePeriod,
      final Optional<OutputRefinement> emitStrategy
  ) {
    super(location, retention, gracePeriod, emitStrategy);
    this.size = requireNonNull(size, "size");
    this.advanceBy = requireNonNull(advanceBy, "advanceBy");
  }

  @JsonIgnore
  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(WindowType.HOPPING, Optional.of(size.toDuration()), emitStrategy);
  }

  public WindowType getWindowType() {
    return WindowType.HOPPING;
  }

  public WindowTimeClause getSize() {
    return size;
  }

  public WindowTimeClause getAdvanceBy() {
    return advanceBy;
  }

  @Override
  public <R, C> R accept(final WindowVisitor<R, C> visitor, final C context) {
    return visitor.visitHoppingWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " HOPPING ( SIZE " + size + " , "
        + "ADVANCE BY " + advanceBy
        + retention.map(w -> " , RETENTION " + w).orElse("")
        + gracePeriod.map(g -> " , GRACE PERIOD " + g).orElse("")
        + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, advanceBy, retention, gracePeriod);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression) o;
    return Objects.equals(size, hoppingWindowExpression.size)
        && Objects.equals(advanceBy, hoppingWindowExpression.advanceBy)
        && Objects.equals(gracePeriod, hoppingWindowExpression.gracePeriod)
        && Objects.equals(retention, hoppingWindowExpression.retention);
  }
}
