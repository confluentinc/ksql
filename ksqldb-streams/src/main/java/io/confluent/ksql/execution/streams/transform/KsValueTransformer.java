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

package io.confluent.ksql.execution.streams.transform;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import java.util.Optional;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * A Kafka-streams value-transformer
 *
 * <p>Maps an implementation agnostic {@link KsqlTransformer} to a implementation specific {@link
 * ValueTransformerWithKey}.
 *
 * @param <K> the type of the key
 * @param <R> the return type
 */
public class KsValueTransformer<K, R> implements ValueTransformerWithKey<K, GenericRow, R> {

  private final KsqlTransformer<K, R> delegate;
  private Optional<KsProcessingContext> context;

  public KsValueTransformer(final KsqlTransformer<K, R> delegate) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.context = Optional.empty();
  }

  @Override
  public void init(final ProcessorContext processorContext) {
    this.context = Optional.of(new KsProcessingContext(processorContext));
  }

  @Override
  public R transform(final K readOnlyKey, final GenericRow value) {
    return delegate.transform(
        readOnlyKey,
        value,
        context.orElseThrow(() -> new IllegalStateException("Not initialized"))
    );
  }

  @Override
  public void close() {
  }

  public static final class KsProcessingContext implements KsqlProcessingContext {

    private final ProcessorContext processingContext;

    public KsProcessingContext(final ProcessorContext processorContext) {
      this.processingContext = requireNonNull(processorContext, "processorContext");
    }

    @Override
    public long getRowTime() {
      return processingContext.timestamp();
    }
  }
}
