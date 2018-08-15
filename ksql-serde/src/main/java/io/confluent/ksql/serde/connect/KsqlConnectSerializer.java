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

import io.confluent.ksql.GenericRow;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

public class KsqlConnectSerializer implements Serializer<GenericRow> {
  private final DataTranslator translator;
  private final Converter converter;

  public KsqlConnectSerializer(final DataTranslator translator,
                               final Converter converter) {
    this.translator = translator;
    this.converter = converter;
  }

  @Override
  public byte[] serialize(final String topic, final GenericRow genericRow) {
    if (genericRow == null) {
      return null;
    }
    final Struct struct = translator.toConnectRow(genericRow);
    try {
      return converter.fromConnectData(topic, struct.schema(), struct);
    } catch (final Exception e) {
      throw new SerializationException(
          "Error serializing row to topic " + topic + " using Converter API", e);
    }
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @Override
  public void close() {
  }
}
