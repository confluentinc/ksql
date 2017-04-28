/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.planner.plan.OutputNode;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;

import java.util.concurrent.SynchronousQueue;

public class QueuedQueryMetadata extends QueryMetadata {
  private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;

  public QueuedQueryMetadata(
      String queryId,
      KafkaStreams queryKafkaStreams,
      OutputNode queryOutputNode,
      SynchronousQueue<KeyValue<String, GenericRow>> rowQueue
  ) {
    super(queryId, queryKafkaStreams, queryOutputNode);
    this.rowQueue = rowQueue;
  }

  public QueuedQueryMetadata(QueryMetadata queryMetadata, SynchronousQueue<KeyValue<String, GenericRow>> rowQueue) {
    this(queryMetadata.getQueryId(), queryMetadata.queryKafkaStreams, queryMetadata.queryOutputNode, rowQueue);
  }

  public SynchronousQueue<KeyValue<String, GenericRow>> getRowQueue() {
    return rowQueue;
  }
}
