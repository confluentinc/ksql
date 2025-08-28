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
import io.confluent.ksql.execution.transform.KsqlTransformer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * A Kafka-streams transformer
 *
 * <p>Maps two implementation agnostic {@link KsqlTransformer KsqlTransformers}
 * (one for the key and one for the value) to an implementation specific {@link Transformer}.
 *
 * @param <KInT> the type of the key
 * @param <KOutT> the return type for the key
 */
public class KsTransformer<KInT, KOutT>
    implements Transformer<KInT, GenericRow, KeyValue<KOutT, GenericRow>> {

  private final KsqlTransformer<KInT, KOutT> keyDelegate;
  private final KsqlTransformer<KInT, GenericRow> valueDelegate;

  public KsTransformer(
      final KsqlTransformer<KInT, KOutT> keyDelegate,
      final KsqlTransformer<KInT, GenericRow> valueDelegate
  ) {
    this.keyDelegate = requireNonNull(keyDelegate, "keyDelegate");
    this.valueDelegate = requireNonNull(valueDelegate, "valueDelegate");
  }

  @Override
  public void init(final ProcessorContext processorContext) {}

  @Override
  public KeyValue<KOutT, GenericRow> transform(final KInT key, final GenericRow value) {
    return KeyValue.pair(
        keyDelegate.transform(
            key,
            value
        ),
        valueDelegate.transform(
            key,
            value
        )
    );
  }

  @Override
  public void close() {}
}
