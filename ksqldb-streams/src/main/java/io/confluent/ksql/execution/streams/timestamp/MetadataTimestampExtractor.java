/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.streams.timestamp;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MetadataTimestampExtractor implements KsqlTimestampExtractor {
  private final TimestampExtractor timestampExtractor;

  public MetadataTimestampExtractor(final TimestampExtractor timestampExtractor) {
    this.timestampExtractor = requireNonNull(timestampExtractor, "timestampExtractor");
  }

  public TimestampExtractor getTimestampExtractor() {
    return timestampExtractor;
  }

  @Override
  public long extract(
      final ConsumerRecord<Object, Object> record,
      final long previousTimestamp
  ) {
    return timestampExtractor.extract(record, previousTimestamp);
  }

  @Override
  public long extract(final Object key, final GenericRow value) {
    throw new UnsupportedOperationException("Operation not supported for class: "
        + timestampExtractor.getClass());
  }
}
