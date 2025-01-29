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

package io.confluent.ksql.execution.streams.process;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;

/**
 * A Kafka-streams value processor
 *
 * <p>Maps an implementation agnostic {@link KsqlTransformer} to an implementation specific {@link
 * Processor}.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public class KsFlatValueProcessor<K, R> implements FixedKeyProcessor<K, GenericRow, R> {

  private final KsqlTransformer<K, Iterable<R>> delegate;
  private FixedKeyProcessorContext<K, R> processorContext;

  public KsFlatValueProcessor(final KsqlTransformer<K, Iterable<R>> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.processorContext = null;
  }

  @Override
  public void init(final FixedKeyProcessorContext<K, R> context) {
    this.processorContext = context;
  }

  @Override
  public void process(final FixedKeyRecord<K, GenericRow> record) {
    final K key = record.key();
    final GenericRow value = record.value();
    if (value == null) {
      processorContext.forward(record.withValue(null));
      return;
    }

    final Iterable<R> result = delegate.transform(
        key,
        value
    );

    if (result == null) {
      processorContext.forward(record.withValue(null));
      return;
    }

    result.forEach(r -> processorContext.forward(record.withValue(r)));
  }

  public static <K> KsFlatValueProcessor<K, GenericRow> of(
      final KsqlTransformer<K, Optional<GenericRow>> delegate) {
    return new KsFlatValueProcessor<>(
        (k, v) -> {
          final Optional<GenericRow> transformedValue = delegate.transform(k, v);
          return Collections.singleton(
              transformedValue.map(Collections::singletonList).orElse(null));
        });
  }
}
