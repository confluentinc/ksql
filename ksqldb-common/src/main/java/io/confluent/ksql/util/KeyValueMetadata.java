package io.confluent.ksql.util;

import java.util.Optional;

public class KeyValueMetadata<K, V> {


  private final KeyValue<K, V> keyValue;
  private final Optional<RowMetadata> progressMetadata;

  public KeyValueMetadata(KeyValue<K, V> keyValue, Optional<RowMetadata> progressMetadata) {
    this.keyValue = keyValue;
    this.progressMetadata = progressMetadata;
  }

  public KeyValue<K, V> getKeyValue() {
    return keyValue;
  }

  public Optional<RowMetadata> getProgressMetadata() {
    return progressMetadata;
  }
}
