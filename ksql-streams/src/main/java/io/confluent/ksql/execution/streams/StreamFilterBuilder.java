/*
 * Copyright 2019 Confluent Inc.
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
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryLoggerUtil;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamFilter;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
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
      final KsqlQueryBuilder queryBuilder) {
    return build(stream, step, queryBuilder, SqlPredicate::new);
  }

  static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFilter<K> step,
      final KsqlQueryBuilder queryBuilder,
      final SqlPredicateFactory predicateFactory
  ) {
    final QueryContext.Stacker contextStacker = QueryContext.Stacker.of(
        step.getProperties().getQueryContext()
    );

    final SqlPredicate predicate = predicateFactory.create(
        step.getFilterExpression(),
        stream.getSchema(),
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()
    );

    final ProcessingLogger processingLogger = queryBuilder
        .getProcessingLogContext()
        .getLoggerFactory()
        .getLogger(
            QueryLoggerUtil.queryLoggerName(
                queryBuilder.getQueryId(),
                contextStacker.push(step.getStepName()).getQueryContext()
            )
        );

    final KStream<K, GenericRow> filtered = stream.getStream()
        .flatTransformValues(
            () -> toFlatMapTransformer(predicate.getTransformer(processingLogger)),
            Named.as(queryBuilder.buildUniqueNodeName(step.getStepName()))
        );

    return stream.withStream(
        filtered,
        stream.getSchema()
    );
  }

  private static <K, V> ValueTransformerWithKey<K, V, Iterable<V>> toFlatMapTransformer(
      final ValueTransformerWithKey<K, V, Optional<V>> transformer
  ) {
    return new ValueTransformerWithKey<K, V, Iterable<V>>() {
      @Override
      public void init(final ProcessorContext context) {
        transformer.init(context);
      }

      @Override
      public Iterable<V> transform(final K readOnlyKey, final V value) {
        final Optional<V> result = transformer.transform(readOnlyKey, value);
        return result
            .map(Collections::singletonList)
            .orElse(Collections.emptyList());
      }

      @Override
      public void close() {
        transformer.close();
      }
    };
  }
}
