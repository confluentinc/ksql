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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LongTimestampExtractor implements TimestampExtractor, Configurable {

  private static final Logger log = LoggerFactory.getLogger(LongTimestampExtractor.class);

  private int timestampColumnindex = -1;

  @Override
  public void configure(Map<String, ?> map) {
    if (map.containsKey(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX)) {
      timestampColumnindex = (Integer) map.get(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX);
    }
  }

  @Override
  public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
    if (timestampColumnindex < 0) {
      return 0;
    } else {
      try {
        if (consumerRecord.value() instanceof GenericRow) {
          GenericRow genericRow = (GenericRow) consumerRecord.value();
          if (genericRow.getColumns().get(timestampColumnindex) instanceof Long) {
            return (long) genericRow.getColumns().get(timestampColumnindex);
          }
        }
      } catch (Exception e) {
        log.error("Exception in extracting timestamp for row: " + consumerRecord.value(), e);
      }
    }
    return 0;
  }
}
