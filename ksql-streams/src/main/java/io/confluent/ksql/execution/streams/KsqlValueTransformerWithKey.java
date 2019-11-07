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
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Base class for all {@code ValueTransformerWithKey} used in KS topologies by KSQL.
 */
public abstract class KsqlValueTransformerWithKey<K>
    implements ValueTransformerWithKey<K, GenericRow, GenericRow> {

  @Override
  public void init(final ProcessorContext processorContext) {
  }

  @Override
  public GenericRow transform(final K key, final GenericRow value) {
    return transform(value);
  }

  protected abstract GenericRow transform(final GenericRow value);

  @Override
  public void close() {
  }
}

