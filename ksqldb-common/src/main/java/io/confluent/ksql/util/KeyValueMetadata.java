package io.confluent.ksql.util;

import com.google.common.base.Strings;
import java.util.Optional;

public class KeyValueMetadata<K, V> {


  private final KeyValue<K, V> keyValue;
  private final String token;

  public KeyValueMetadata(KeyValue<K, V> keyValue, String token) {
    this.keyValue = keyValue;
    this.token = token;
  }

  public KeyValue<K, V> getKeyValue() {
    return keyValue;
  }

  public Optional<String> getToken() {
    return Optional.ofNullable(Strings.emptyToNull(token));
  }
}
