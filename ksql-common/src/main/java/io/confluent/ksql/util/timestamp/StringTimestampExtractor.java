/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util.timestamp;

import com.google.common.base.Preconditions;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class StringTimestampExtractor implements TimestampExtractor {
  private final StringToTimestampParser timestampParser;
  private final int timestampColumn;
  private final String format;


  StringTimestampExtractor(final String format, final int timestampColumn) {
    this.format = Objects.requireNonNull(format, "format can't be null");
    Preconditions.checkArgument(timestampColumn >= 0, "timestampColumn must be >= 0");
    this.timestampColumn = timestampColumn;
    this.timestampParser = new StringToTimestampParser(format);

  }

  @Override
  public long extract(final ConsumerRecord<Object, Object> consumerRecord,
                      final long previousTimestamp) {
    final GenericRow row = (GenericRow) consumerRecord.value();
    final String value = row.getColumnValue(timestampColumn);
    try {
      return timestampParser.parse(value);
    } catch (final KsqlException e) {
      throw new KsqlException("Unable to parse string timestamp from record."
          + " record=" + consumerRecord
          + " timestamp=" + value
          + " timestamp_format=" + format,
          e);
    }
  }

}
