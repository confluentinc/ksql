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
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
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
public class KsFixedKeyProcessor<K, R> implements FixedKeyProcessor<K, GenericRow, R> {
  private final KsqlTransformer<K, R> delegate;
  private FixedKeyProcessorContext<K, R> processorContext;
  private KsqlProcessingContext ksqlProcessingContext;

  public KsFixedKeyProcessor(final KsqlTransformer<K, R> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.processorContext = null;
  }

  @Override
  public void init(final FixedKeyProcessorContext<K, R> context) {
    this.processorContext = context;
    this.ksqlProcessingContext = new KsStreamProcessingContext(context);
  }

  @Override
  public void process(final FixedKeyRecord<K, GenericRow> record) {
    final K key = record.key();
    final GenericRow value = record.value();
    final R result = delegate.transform(
        key,
        value,
        ksqlProcessingContext
    );
    processorContext.forward(record.withValue(result));
  }
}
