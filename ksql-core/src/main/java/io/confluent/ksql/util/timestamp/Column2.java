/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util.timestamp;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Column2 extends KsqlTimespampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
    return extractTime(consumerRecord, 2);
  }
}