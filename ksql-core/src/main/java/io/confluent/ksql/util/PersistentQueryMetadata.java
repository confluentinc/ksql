/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import io.confluent.ksql.planner.plan.OutputNode;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Objects;

public class PersistentQueryMetadata extends QueryMetadata {

  private final long id;

  public PersistentQueryMetadata(String statementString, KafkaStreams kafkaStreams, OutputNode outputNode, long id) {
    super(statementString, kafkaStreams, outputNode);
    this.id = id;
  }

  public long getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PersistentQueryMetadata)) {
      return false;
    }

    PersistentQueryMetadata that = (PersistentQueryMetadata) o;

    return Objects.equals(this.id, that.id) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, super.hashCode());
  }
}
