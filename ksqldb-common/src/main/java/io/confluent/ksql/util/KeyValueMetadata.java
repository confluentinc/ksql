package io.confluent.ksql.util;

public class KeyValueMetadata<K, V> {


  private final KeyValue<K, V> keyValue;
  private final int partition;
  private final long offset;

  public KeyValueMetadata(KeyValue<K, V> keyValue, int partition, long offset) {
    this.keyValue = keyValue;
    this.partition = partition;
    this.offset = offset;
  }

  public KeyValue<K, V> getKeyValue() {
    return keyValue;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }
}
