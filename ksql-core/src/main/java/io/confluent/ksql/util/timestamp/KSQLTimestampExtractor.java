/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util.timestamp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.KSQLConfig;

public class KSQLTimestampExtractor implements TimestampExtractor, Configurable {

  private static final Logger log = LoggerFactory.getLogger(KSQLTimestampExtractor.class);

  int timestampColumnindex = -1;

  @Override
  public void configure(Map<String, ?> map) {
    if (map.containsKey(KSQLConfig.KSQL_TIMESTAMP_COLUMN_INDEX)) {
      timestampColumnindex = (Integer) map.get(KSQLConfig.KSQL_TIMESTAMP_COLUMN_INDEX);
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
