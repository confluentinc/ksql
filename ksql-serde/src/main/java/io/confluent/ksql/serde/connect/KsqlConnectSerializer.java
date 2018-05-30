/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.connect;

import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.util.SerdeUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public class KsqlConnectSerializer implements Serializer<GenericRow> {
  private final Schema schema;
  private final Converter converter;

  public KsqlConnectSerializer(final Schema schema, final Converter converter) {
    this.schema = SerdeUtils.toOptionalSchema(schema);
    this.converter = converter;
  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }

    try {
      final Struct struct = new Struct(schema);
      Streams.forEachPair(
          schema.fields().stream(),
          genericRow.getColumns().stream(),
          (field, columnValue) -> struct.put(field, columnValue));
      return converter.fromConnectData(topic, schema, struct);
    } catch (Exception e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public void close() {
  }
}
