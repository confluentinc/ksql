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

package io.confluent.ksql.serde.unwrapped;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.serde.SerdeUtils;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public class UnwrappedDeserializer<T> implements Deserializer<Struct> {

  private final Field field;
  private final ConnectSchema schema;
  private final Deserializer<T> inner;

  public UnwrappedDeserializer(
      final ConnectSchema schema,
      final Deserializer<T> inner,
      final Class<T> colType
  ) {
    this.schema = requireNonNull(schema, "schema");
    this.inner = requireNonNull(inner, "inner");
    this.field = Unwrapped.getOnlyField(schema);

    SerdeUtils.throwOnSchemaJavaTypeMismatch(field.schema(), colType);
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public Struct deserialize(final String topic, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    final T single = inner.deserialize(topic, bytes);
    return wrapSingle(single);
  }

  @Override
  public Struct deserialize(final String topic, final Headers headers, final byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    final Object single = inner.deserialize(topic, headers, bytes);
    return wrapSingle(single);
  }

  @Override
  public void close() {
    inner.close();
  }

  private Struct wrapSingle(final Object single) {
    return new Struct(schema).put(field, single);
  }
}

