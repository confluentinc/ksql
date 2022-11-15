/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.engine.generic;

import io.confluent.ksql.GenericRow;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;

/**
 * Represents a record in ksqlDB which consists of a {@link Struct} key,
 * a {@link GenericRow} value and a {@code long} timestamp. This is mostly
 * used when generating data from SQL expressions.
 */
public final class KsqlGenericRecord {

  public final Struct key;
  public final GenericRow value;
  public final long ts;

  public static KsqlGenericRecord of(
      final Struct key,
      final GenericRow value,
      final long ts
  ) {
    return new KsqlGenericRecord(key, value, ts);
  }

  private KsqlGenericRecord(
      final Struct key,
      final GenericRow value,
      final long ts
  ) {
    this.key = key;
    this.value = value;
    this.ts = ts;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlGenericRecord that = (KsqlGenericRecord) o;
    return ts == that.ts
        && Objects.equals(key, that.key)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, ts);
  }

  @Override
  public String toString() {
    return "KsqlGenericRecord{"
        + "key=" + key
        + ", value=" + value
        + ", ts=" + ts
        + '}';
  }
}
