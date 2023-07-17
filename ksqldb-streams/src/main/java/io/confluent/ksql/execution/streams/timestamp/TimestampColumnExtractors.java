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

package io.confluent.ksql.execution.streams.timestamp;

import com.google.common.base.Preconditions;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Extracts a column from the ConsumerRecord
 */
final class TimestampColumnExtractors {

  private TimestampColumnExtractors() {
  }

  static ColumnExtractor create(final Column column) {
    final int index = column.index();
    Preconditions.checkArgument(index >= 0, "negative index: " + index);

    return column.namespace() == Namespace.KEY
        ? new KeyColumnExtractor(index)
        : new ValueColumnExtractor(index);
  }

  private static class KeyColumnExtractor implements ColumnExtractor {

    private final int index;

    KeyColumnExtractor(final int index) {
      this.index = index;
    }

    @Override
    public Object extract(final Object key, final GenericRow value) {
      final GenericKey genericKey = getGenericKey(key);
      return genericKey.get(index);
    }

    private static GenericKey getGenericKey(final Object key) {
      if (key instanceof Windowed) {
        final Windowed<?> windowed = (Windowed<?>) key;
        return (GenericKey) windowed.key();
      }
      return (GenericKey) key;
    }
  }

  private static class ValueColumnExtractor implements ColumnExtractor {

    private final int index;

    ValueColumnExtractor(final int index) {
      this.index = index;
    }

    @Override
    public Object extract(final Object key, final GenericRow value) {
      return value.get(index);
    }
  }
}

