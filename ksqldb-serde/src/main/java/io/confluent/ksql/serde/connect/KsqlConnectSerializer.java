/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.serde.KsqlSerializationException;
import io.confluent.ksql.serde.SerdeUtils;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.storage.Converter;

public class KsqlConnectSerializer<T> implements Serializer<T> {

  private final Schema schema;
  private final DataTranslator translator;
  private final Converter converter;

  public KsqlConnectSerializer(
      final Schema schema,
      final DataTranslator translator,
      final Converter converter,
      final Class<T> targetType
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.translator = Objects.requireNonNull(translator, "translator");
    this.converter = Objects.requireNonNull(converter, "converter");

    SerdeUtils.throwOnSchemaJavaTypeMismatch(schema, targetType);
  }

  @Override
  public byte[] serialize(final String topic, final T data) {
    if (data == null) {
      return null;
    }

    try {
      final Object connectRow = translator.toConnectRow(data);
      return converter.fromConnectData(topic, schema, connectRow);
    } catch (final Exception e) {
      throw new KsqlSerializationException(
          topic, "Error serializing message to topic: " + topic + ". " + e.getMessage(), e);
    }
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public void close() {
  }
}
