/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util.timestamp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.ksql.physical.GenericRow;

public abstract class KsqlTimespampExtractor implements TimestampExtractor {

  private static final Logger log = LoggerFactory.getLogger(KsqlTimespampExtractor.class);

  long extractTime(ConsumerRecord<Object, Object> consumerRecord, int columnIndex) {
    try {
      if (consumerRecord.value() instanceof GenericRow) {
        GenericRow genericRow = (GenericRow) consumerRecord.value();
        if (genericRow.getColumns().get(columnIndex) instanceof Long) {
          return (long) genericRow.getColumns().get(columnIndex);
        }
      }
    } catch (Exception e) {
      // Hanlde the exception.
      log.error("Exception in extracting timestamp for row: " + consumerRecord.value(), e);
    }
    return 0;
  }

}
