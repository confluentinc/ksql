/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import org.apache.kafka.streams.KafkaStreams;

import io.confluent.kql.planner.plan.OutputNode;

public class QueryMetadata {
  final String queryId;
  final KafkaStreams queryKafkaStreams;
  final OutputNode queryOutputNode;

  public QueryMetadata (final String queryId, final KafkaStreams queryKafkaStreams, final OutputNode queryOutputNode) {
    this.queryId = queryId;
    this.queryKafkaStreams = queryKafkaStreams;
    this.queryOutputNode = queryOutputNode;
  }

  public String getQueryId() {
    return queryId;
  }

  public KafkaStreams getQueryKafkaStreams() {
    return queryKafkaStreams;
  }

  public OutputNode getQueryOutputNode() {
    return queryOutputNode;
  }
}
