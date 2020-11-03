/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.util.Objects;

public class KeyValue<K, V> {

  private final K key;
  private final V value;

  public KeyValue(final K key, final V value) {
    this.key = key;
    this.value = value;
  }

  public static <K, V> KeyValue<K, V> keyValue(final K key, final V value) {
    return new KeyValue<>(key, value);
  }

  public K key() {
    return key;
  }

  public V value() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KeyValue<?, ?> keyValue = (KeyValue<?, ?>) o;
    return Objects.equals(key, keyValue.key)
        && Objects.equals(value, keyValue.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }

  public String toString() {
    return "(" + this.key + ", " + this.value + ")";
  }
}
