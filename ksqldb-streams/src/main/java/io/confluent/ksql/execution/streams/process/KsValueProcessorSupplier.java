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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Supplies {@link KsValueProcessor} instances for processing values in a Kafka Streams pipeline.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public class KsValueProcessorSupplier<K, R>
    implements ProcessorSupplier<K, GenericRow, K, R> {

  private final KsqlTransformer<K, R> delegate;

  public KsValueProcessorSupplier(final KsqlTransformer<K, R> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
  }

  @Override
  public Processor<K, GenericRow, K, R> get() {
    return new KsValueProcessor<>(delegate);
  }
}
