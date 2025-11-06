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
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/**
 * A Kafka-streams value processor that flattens the transformed value.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public class KsFlatTransformedValueProcessor<K, R> implements FixedKeyProcessor<K, GenericRow, R> {

  private final KsqlTransformer<K, Iterable<R>> delegate;
  private FixedKeyProcessorContext<K, R> processorContext;

  public KsFlatTransformedValueProcessor(final KsqlTransformer<K, Iterable<R>> delegate) {
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
      return;
    }

    final Iterable<R> result = delegate.transform(
        key,
        value
    );

    if (result == null) {
      return;
    }

    result.forEach(r -> processorContext.forward(record.withValue(r)));
  }
}
