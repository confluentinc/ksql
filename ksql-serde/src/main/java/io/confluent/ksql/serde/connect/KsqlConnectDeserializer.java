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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public class KsqlConnectDeserializer implements Deserializer<GenericRow> {
  final Schema schema;
  final Converter converter;
  final ConnectDataTranslator connectToKsqlTranslator;

  public KsqlConnectDeserializer(
      final Schema schema,
      final Converter converter,
      final ConnectDataTranslator connectToKsqlTranslator) {
    this.schema = schema;
    this.converter = converter;
    this.connectToKsqlTranslator = connectToKsqlTranslator;
  }

  @Override
  public void configure(final Map<String, ?> map, final boolean b) {
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    final SchemaAndValue schemaAndValue = converter.toConnectData(topic, bytes);
    return connectToKsqlTranslator.toKsqlRow(
        schema, schemaAndValue.schema(), schemaAndValue.value());
  }

  @Override
  public void close() {
  }
}
