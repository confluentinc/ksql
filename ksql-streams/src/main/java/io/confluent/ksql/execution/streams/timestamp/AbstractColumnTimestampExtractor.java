/*
 * Copyright 2020 Confluent Inc.
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

import com.google.common.base.Preconditions;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public abstract class AbstractColumnTimestampExtractor implements TimestampExtractor {
  protected final int timetampColumnIndex;

  AbstractColumnTimestampExtractor(final int timestampColumnindex) {
    Preconditions.checkArgument(timestampColumnindex >= 0, "timestampColumnindex must be >= 0");
    this.timetampColumnIndex = timestampColumnindex;
  }

  @Override
  public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
    if (record.value() instanceof GenericRow) {
      try {
        return extract((GenericRow) record.value());
      } catch (final Exception e) {
        throw new KsqlException("Unable to extract timestamp from record."
            + " record=" + record,
            e);
      }
    }

    return 0;
  }

  public abstract long extract(GenericRow row);
}
