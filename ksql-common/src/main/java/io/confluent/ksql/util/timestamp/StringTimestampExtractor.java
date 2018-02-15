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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import io.confluent.common.Configurable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

public class StringTimestampExtractor implements TimestampExtractor, Configurable {
  private SimpleDateFormat dateFormatter;
  private int timestampColumn = -1;
  private String format;

  @Override
  public long extract(final ConsumerRecord<Object, Object> consumerRecord,
                      final long previousTimestamp) {
    final GenericRow row = (GenericRow) consumerRecord.value();
    final String value = row.getColumnValue(timestampColumn);
    try {
      return dateFormatter.parse(value).getTime();
    } catch (ParseException e) {
      throw new KsqlException("Unable to parse string timestamp from record."
          + " record=" + consumerRecord
          + " timestamp=" + value
          + " timestamp_format=" + format,
          e);
    }
  }

  @Override
  public void configure(final Map<String, ?> map) {
    format = (String) map.get(StringTimestampExtractionPolicy.STRING_TIMESTAMP_FORMAT);
    if (format == null) {
      throw new IllegalArgumentException("Value of "
          + StringTimestampExtractionPolicy.STRING_TIMESTAMP_FORMAT
          + " must not be null");
    }
    final Integer index = (Integer) map.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX);
    if (index == null || index < 0) {
      throw new IllegalArgumentException("Value of "
          + KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX
          + " must be an integer >= 0");
    }
    this.timestampColumn = index;
    this.dateFormatter = new SimpleDateFormat(
        format);
  }
}
