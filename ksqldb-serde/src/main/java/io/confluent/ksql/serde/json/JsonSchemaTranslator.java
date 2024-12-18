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

package io.confluent.ksql.serde.json;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

/**
 * Translates between Connect and JSON Schema Registry schema types.
 */
public class JsonSchemaTranslator implements ConnectSchemaTranslator {

  private final JsonSchemaData jsonData = new JsonSchemaData(new JsonSchemaDataConfig(
      ImmutableMap.of(
          JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
      )
  ));

  @Override
  public void configure(final Map<String, ?> configs) {
  }

  @Override
  public String name() {
    return JsonSchema.TYPE;
  }

  @Override
  public Schema toConnectSchema(final ParsedSchema schema) {
    return jsonData.toConnectSchema((JsonSchema) schema);
  }

  @Override
  public ParsedSchema fromConnectSchema(final Schema schema) {
    return jsonData.fromConnectSchema(schema);
  }
}
