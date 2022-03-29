/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.execution.streams.transform.KsValueTransformer;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

public final class StreamFilterBuilder {
  private StreamFilterBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFilter<K> step,
      final RuntimeBuildContext buildContext) {
    return build(stream, step, buildContext, SqlPredicate::new);
  }

  static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFilter<K> step,
      final RuntimeBuildContext buildContext,
      final SqlPredicateFactory predicateFactory
  ) {
    final SqlPredicate predicate = predicateFactory.create(
        step.getFilterExpression(),
        stream.getSchema(),
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );

    final ProcessingLogger processingLogger = buildContext
        .getProcessingLogger(step.getProperties().getQueryContext());

    final KStream<K, GenericRow> filtered = stream.getStream()
        .flatTransformValues(
            () -> toFlatMapTransformer(predicate.getTransformer(processingLogger)),
            Named.as(StreamsUtil.buildOpName(step.getProperties().getQueryContext()))
        );

    return stream.withStream(
        filtered,
        stream.getSchema()
    );
  }

  private static <K> ValueTransformerWithKey<
      K,
      GenericRow,
      Iterable<GenericRow>
      > toFlatMapTransformer(
          final KsqlTransformer<K, Optional<GenericRow>> transformer
  ) {
    final ValueTransformerWithKey<K, GenericRow, Optional<GenericRow>> delegate =
        new KsValueTransformer<>(transformer);

    return new ValueTransformerWithKey<K, GenericRow, Iterable<GenericRow>>() {
      @Override
      public void init(final ProcessorContext context) {
        delegate.init(context);
      }

      @Override
      public Iterable<GenericRow> transform(final K readOnlyKey, final GenericRow value) {
        final Optional<GenericRow> result = delegate.transform(readOnlyKey, value);
        return result
            .map(Collections::singletonList)
            .orElse(Collections.emptyList());
      }

      @Override
      public void close() {
        delegate.close();
      }
    };
  }
}
