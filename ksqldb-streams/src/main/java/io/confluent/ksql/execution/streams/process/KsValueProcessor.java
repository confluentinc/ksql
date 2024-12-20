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
import io.confluent.ksql.execution.process.KsqlProcessor;
import java.util.Optional;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * A Kafka-streams value processor
 *
 * <p>Maps an implementation agnostic {@link KsqlProcessor} to an implementation specific {@link
 * Processor}.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public class KsValueProcessor<K, R> implements Processor<K, GenericRow, K, R> {
  private final KsqlProcessor<K, R> delegate;
  private ProcessorContext<K, R> context;

  public KsValueProcessor(final KsqlProcessor<K, R> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.context = null;
  }

  @Override
  public void init(final ProcessorContext<K, R> context) {
    this.context = context;
  }

  @Override
  public void process(final Record<K, GenericRow> record) {
    final K key = record.key();
    final GenericRow value = record.value();
    final R result = delegate.process(
        key,
        value,
        context
    );
    context.forward(new Record<>(key, result, record.timestamp()));
  }

}
