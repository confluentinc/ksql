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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;

@Immutable
public class SessionWindowExpression extends KsqlWindowExpression {

  private final long gap;
  private final TimeUnit sizeUnit;

  public SessionWindowExpression(final long gap, final TimeUnit sizeUnit) {
    this(Optional.empty(), gap, sizeUnit);
  }

  public SessionWindowExpression(
      final Optional<NodeLocation> location,
      final long gap,
      final TimeUnit sizeUnit
  ) {
    super(location);
    this.gap = gap;
    this.sizeUnit = requireNonNull(sizeUnit, "sizeUnit");
  }

  @Override
  public WindowInfo getWindowInfo() {
    return WindowInfo.of(WindowType.SESSION, Optional.empty());
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitSessionWindowExpression(this, context);
  }

  @Override
  public String toString() {
    return " SESSION ( " + gap + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(gap, sizeUnit);
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
    return sessionWindowExpression.gap == gap && sessionWindowExpression.sizeUnit == sizeUnit;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KTable applyAggregate(final KGroupedStream groupedStream,
                               final Initializer initializer,
                               final UdafAggregator aggregator,
                               final Materialized<Struct, GenericRow, ?> materialized) {

    final SessionWindows windows = SessionWindows.with(Duration.ofMillis(sizeUnit.toMillis(gap)));

    return groupedStream
        .windowedBy(windows)
        .aggregate(initializer, aggregator, aggregator.getMerger(), materialized);
  }
}
