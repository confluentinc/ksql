/*
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
 */

package io.confluent.ksql.logging.processing;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.logging.StructuredLogMessage;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

// This is from common repo. We need it for converting our message to StructuredLogMessage.
final class SerializableSchemaAndValue implements StructuredLogMessage {
  private static final JsonConverter converter = new JsonConverter();

  static {
    converter.configure(
        ImmutableMap.of(
            JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false
        ),
        false
    );
  }

  private final SchemaAndValue schemaAndValue;
  private volatile String asString = null;

  SerializableSchemaAndValue(final SchemaAndValue schemaAndValue) {
    this.schemaAndValue = schemaAndValue;
  }

  public SchemaAndValue getMessage() {
    return schemaAndValue;
  }

  private String serializeToString() {
    final byte[] bytes = converter.fromConnectData(
        "",
        schemaAndValue.schema(),
        schemaAndValue.value());
    // converter encodes as utf-8
    return new String(bytes, Charsets.UTF_8);
  }

  public String toString() {
    if (asString == null) {
      asString = serializeToString();
    }
    return asString;
  }
}