/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.planner.plan.OutputNode;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;

import java.util.Objects;
import java.util.concurrent.SynchronousQueue;

public class QueuedQueryMetadata extends QueryMetadata {

  private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;

  public QueuedQueryMetadata(
      String statementString,
      KafkaStreams kafkaStreams,
      OutputNode outputNode,
      SynchronousQueue<KeyValue<String, GenericRow>> rowQueue
  ) {
    super(statementString, kafkaStreams, outputNode);
    this.rowQueue = rowQueue;
  }

  public SynchronousQueue<KeyValue<String, GenericRow>> getRowQueue() {
    return rowQueue;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QueuedQueryMetadata)) {
      return false;
    }

    QueuedQueryMetadata that = (QueuedQueryMetadata) o;

    return Objects.equals(this.rowQueue, that.rowQueue) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rowQueue, super.hashCode());
  }
}
