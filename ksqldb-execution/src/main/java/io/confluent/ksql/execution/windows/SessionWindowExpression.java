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
public class SessionWindowExpression extends KsqlWindowExpression {

  private final WindowTimeClause gap;

  @JsonCreator
  public static SessionWindowExpression of(
      @JsonProperty(value = "gap", required = true) final WindowTimeClause gap,
      @JsonProperty(value = "retention") final WindowTimeClause retention,
      @JsonProperty(value = "gracePeriod") final WindowTimeClause gracePeriod,
      @JsonProperty(value = "emitStrategy") final OutputRefinement emitStrategy
  ) {
    return new SessionWindowExpression(
        Optional.empty(),
        gap,
        Optional.ofNullable(retention),
        Optional.ofNullable(gracePeriod),
        Optional.ofNullable(emitStrategy)
    );
  }

  public SessionWindowExpression(final WindowTimeClause gap) {
    this(Optional.empty(), gap, Optional.empty(), Optional.empty());
  }

  public SessionWindowExpression(
      final Optional<NodeLocation> location,
      final WindowTimeClause gap,
      final Optional<WindowTimeClause> retention,
      final Optional<WindowTimeClause> gracePeriod
  ) {
    this(location, gap, retention, gracePeriod, Optional.empty());
  }

  public SessionWindowExpression(
      final Optional<NodeLocation> location,
      final WindowTimeClause gap,
      final Optional<WindowTimeClause> retention,
      final Optional<WindowTimeClause> gracePeriod,
      final Optional<OutputRefinement> emitStrategy
  ) {
    super(location, retention, gracePeriod, emitStrategy);
    this.gap = requireNonNull(gap, "gap");
  }

  public WindowTimeClause getGap() {
    return gap;
  }

  @JsonIgnore
  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(WindowType.SESSION, Optional.empty(), emitStrategy);
  }

  public WindowType getWindowType() {
    return WindowType.SESSION;
  }


  @Override
  public <R, C> R accept(final WindowVisitor<R, C> visitor, final C context) {
    return visitor.visitSessionWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " SESSION ( " + gap
        + retention.map(w -> " , RETENTION " + w).orElse("")
        + gracePeriod.map(g -> " , GRACE PERIOD " + g).orElse("")
        + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(gap, retention, gracePeriod);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SessionWindowExpression sessionWindowExpression = (SessionWindowExpression) o;
    return Objects.equals(gap, sessionWindowExpression.gap)
        && Objects.equals(gracePeriod, sessionWindowExpression.gracePeriod)
        && Objects.equals(retention, sessionWindowExpression.retention);
  }
}
