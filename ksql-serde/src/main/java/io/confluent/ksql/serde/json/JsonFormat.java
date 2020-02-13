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

package io.confluent.ksql.serde.json;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverterConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import org.apache.kafka.connect.data.Schema;

public class JsonFormat implements Format {

  public static final String NAME = JsonSchema.TYPE;

  private JsonSchemaConverter converter;

  public JsonFormat() {
    // we don't actually use schema registry at all in this
    // class - when schema registry
    final ImmutableMap<String, String> config = ImmutableMap.of(
        ProtobufConverterConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "foo"
    );

    this.converter = new JsonSchemaConverter();
    this.converter.configure(config, false);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public boolean supportsSchemaInference() {
    return true;
  }

  @Override
  public Schema toConnectSchema(final ParsedSchema schema) {
    return converter.toConnectSchema((JsonSchema) schema);
  }

  @Override
  public ParsedSchema toParsedSchema(final Schema schema) {
    return converter.fromConnectSchema(schema);
  }

  @Override
  public KsqlSerdeFactory getSerdeFactory(final FormatInfo info) {
    return new KsqlJsonSerdeFactory();
  }
}
