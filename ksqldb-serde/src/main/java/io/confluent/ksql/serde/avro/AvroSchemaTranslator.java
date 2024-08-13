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

package io.confluent.ksql.serde.avro;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.connect.data.Schema;

/**
 * Translates between Connect and Avro Schema Registry schema types.
 */
class AvroSchemaTranslator implements ConnectSchemaTranslator {

  private AvroData avroData = new AvroData(new AvroDataConfig(ImmutableMap.of()));

  private final AvroProperties formatProps;

  AvroSchemaTranslator(final AvroProperties formatProps) {
    this.formatProps = Objects.requireNonNull(formatProps, "formatProps");
  }

  @Override
  public String name() {
    return AvroSchema.TYPE;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    avroData = new AvroData(new AvroDataConfig(configs));
  }

  @Override
  public Schema toConnectSchema(final ParsedSchema schema) {
    return avroData.toConnectSchema(((AvroSchema) schema).rawSchema());
  }

  @Override
  public ParsedSchema fromConnectSchema(final Schema schema) {
    final Schema avroCompatibleSchema = AvroSchemas
        .getAvroCompatibleConnectSchema(schema, formatProps.getFullSchemaName());

    try {
      return new AvroSchema(avroData.fromConnectSchema(avroCompatibleSchema));
    } catch (final SchemaParseException e) {
      throw new KsqlException("Schema is not compatible with Avro: " + e.getMessage(), e);
    }
  }
}
