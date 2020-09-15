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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

/**
 * Serializer that extracts the single column within a {@link Struct} and passes this to an inner
 * serializer.
 */
public class UnwrappedSerializer<T> implements Serializer<Struct> {

  private final Serializer<T> inner;
  private final Field field;

  public UnwrappedSerializer(
      final ConnectSchema schema,
      final Serializer<T> inner,
      final Class<T> colType
  ) {
    this.inner = requireNonNull(inner, "inner");
    this.field = Unwrapped.getOnlyField(schema);

    SerdeUtils.throwOnSchemaJavaTypeMismatch(field.schema(), colType);
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(final String topic, final Struct struct) {
    if (struct == null) {
      return null;
    }

    final T single = extractOnlyColumn(struct);
    return inner.serialize(topic, single);
  }

  @Override
  public byte[] serialize(final String topic, final Headers headers, final Struct struct) {
    if (struct == null) {
      return null;
    }

    final T single = extractOnlyColumn(struct);
    return inner.serialize(topic, headers, single);
  }

  @Override
  public void close() {
    inner.close();
  }

  private T extractOnlyColumn(final Struct struct) {
    final Object val = struct.get(field);
    return castColValue(val);
  }

  @SuppressWarnings("unchecked")
  private T castColValue(final Object val) {
    // Cast is safe as constructor has confirmed the Java type of the field matches T.
    // And ksqlDB ensures only struct's with correct schema are passed.
    return (T) val;
  }
}

