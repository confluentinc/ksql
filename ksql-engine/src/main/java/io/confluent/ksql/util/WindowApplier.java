/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.KsqlWindowExpression;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;

public final class WindowApplier {

  public static KTable applyWindow(
      final KsqlWindowExpression ksqlWindowExpression,
      final KGroupedStream groupedStream,
      final Initializer initializer,
      final UdafAggregator aggregator,
      final Materialized<String, GenericRow, ?> materialized
  ) {
    switch (ksqlWindowExpression.getWindowType()) {
      case TUMBLING:
        return applyTumblingWindow(
            (TumblingWindowExpression) ksqlWindowExpression,
            groupedStream,
            initializer,
            aggregator,
            materialized
        );
      case HOPPING:
        return applyHoppingWindow(
            (HoppingWindowExpression) ksqlWindowExpression,
            groupedStream,
            initializer,
            aggregator,
            materialized
        );
      case SESSION:
        return applySessionWindow(
            (SessionWindowExpression) ksqlWindowExpression,
            groupedStream,
            initializer,
            aggregator,
            materialized
        );
      default:
        throw new KsqlException("Invalid window type: " + ksqlWindowExpression.getWindowType());
    }
  }

  @SuppressWarnings("unchecked")
  private static KTable applyHoppingWindow(
      final HoppingWindowExpression hoppingWindowExpression,
      final KGroupedStream groupedStream,
      final Initializer initializer,
      final UdafAggregator aggregator,
      final Materialized<String, GenericRow, ?> materialized
  ) {
    return groupedStream.windowedBy(
        TimeWindows.of(hoppingWindowExpression.getSizeUnit().toMillis(
            hoppingWindowExpression.getSize()
        )).advanceBy(hoppingWindowExpression.getAdvanceByUnit().toMillis(
            hoppingWindowExpression.getAdvanceBy()
        ))
    ).aggregate(initializer, aggregator, materialized);
  }

  @SuppressWarnings("unchecked")
  private static KTable applySessionWindow(
      final SessionWindowExpression sessionWindowExpression,
      final KGroupedStream groupedStream,
      final Initializer initializer,
      final UdafAggregator aggregator,
      final Materialized<String, GenericRow, ?> materialized
  ) {
    return groupedStream.windowedBy(SessionWindows.with(sessionWindowExpression.getSizeUnit()
        .toMillis(sessionWindowExpression.getGap())))
        .aggregate(initializer, aggregator, aggregator.getMerger(),
            materialized);
  }

  @SuppressWarnings("unchecked")
  private static KTable applyTumblingWindow(
      final TumblingWindowExpression tumblingWindowExpression,
      final KGroupedStream groupedStream,
      final Initializer initializer,
      final UdafAggregator aggregator,
      final Materialized<String, GenericRow, ?> materialized
  ) {
    return groupedStream.windowedBy(TimeWindows.of(tumblingWindowExpression.getSizeUnit()
        .toMillis(tumblingWindowExpression.getSize())))
        .aggregate(initializer, aggregator, materialized);
  }

}
