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

package io.confluent.ksql.serde;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serializer;

class GenericSerializer<T> implements Serializer<T> {

  private final Function<T, List<?>> accessor;
  private final Serializer<List<?>> inner;
  private final int numColumns;

  GenericSerializer(
      final Function<T, List<?>> accessor,
      final Serializer<List<?>> inner,
      final int expectedNumColumns
  ) {
    this.inner = requireNonNull(inner, "inner");
    this.accessor = requireNonNull(accessor, "accessor");
    this.numColumns = expectedNumColumns;

    Preconditions.checkArgument(
        expectedNumColumns >= 0,
        "negative expected column count: " + expectedNumColumns
    );
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(final String topic, final T data) {
    if (data == null) {
      return inner.serialize(topic, null);
    }

    final List<?> list = accessor.apply(data);

    SerdeUtils.throwOnColumnCountMismatch(numColumns, list.size(), true, topic);

    return inner.serialize(topic, list);
  }

  @Override
  public void close() {
    inner.close();
  }
}
