/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import java.util.Optional;

/**
 * A holder for either a KeyValue or any metadata.
 * @param <K> The key type
 * @param <V> The value type
 */
public class KeyValueMetadata<K, V> {

  private final Optional<KeyValue<K, V>> keyValue;
  private final Optional<RowMetadata> rowMetadata;

  public KeyValueMetadata(final KeyValue<K, V> keyValue) {
    this.keyValue = Optional.of(keyValue);
    this.rowMetadata = Optional.empty();
  }

  public KeyValueMetadata(final RowMetadata rowMetadata) {
    this.keyValue = Optional.empty();
    this.rowMetadata = Optional.of(rowMetadata);
  }

  public Optional<KeyValue<K, V>> getKeyValue() {
    return keyValue;
  }

  public Optional<RowMetadata> getRowMetadata() {
    return rowMetadata;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KeyValueMetadata<?, ?> keyValueMetadata = (KeyValueMetadata<?, ?>) o;
    return Objects.equals(keyValue, keyValueMetadata.keyValue)
        && Objects.equals(rowMetadata, keyValueMetadata.rowMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyValue, rowMetadata);
  }
}
