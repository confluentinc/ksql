package io.confluent.ksql.physical.scalablepush.consumer;

public class ConsumerMetadata implements AutoCloseable {

  private final int numPartitions;

  public ConsumerMetadata(final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  @Override
  public void close() {
  }
}
