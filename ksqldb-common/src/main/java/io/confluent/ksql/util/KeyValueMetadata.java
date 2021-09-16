package io.confluent.ksql.util;

import com.google.common.base.Strings;
import java.util.Optional;

public class KeyValueMetadata<K, V> {


  private final KeyValue<K, V> keyValue;
  private final Optional<ProgressMetadata> progressMetadata;

  public KeyValueMetadata(KeyValue<K, V> keyValue, Optional<ProgressMetadata> progressMetadata) {
    this.keyValue = keyValue;
    this.progressMetadata = progressMetadata;
  }

  public KeyValue<K, V> getKeyValue() {
    return keyValue;
  }

  public Optional<ProgressMetadata> getProgressMetadata() {
    return progressMetadata;
  }
}
