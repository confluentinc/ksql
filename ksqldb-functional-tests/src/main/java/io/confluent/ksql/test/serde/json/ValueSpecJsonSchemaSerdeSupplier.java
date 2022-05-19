/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.test.serde.json;

import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.serde.connect.ConnectSchemas;
import io.confluent.ksql.serde.json.JsonSchemaTranslator;
import io.confluent.ksql.test.serde.ConnectSerdeSupplier;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

public class ValueSpecJsonSchemaSerdeSupplier extends ConnectSerdeSupplier<JsonSchema> {
  private final JsonSchemaTranslator schemaTranslator;
  private final LogicalSchema logicalSpecSchema;

  public ValueSpecJsonSchemaSerdeSupplier(final LogicalSchema logicalSpecSchema) {
    super(JsonSchemaConverter::new);
    this.schemaTranslator = new JsonSchemaTranslator();
    this.logicalSpecSchema = logicalSpecSchema;
  }

  @Override
  protected Schema fromParsedSchema(final JsonSchema schema) {
    return schemaTranslator.toConnectSchema(schema);
  }

  @Override
  public Deserializer<Object> getDeserializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    final Schema outerSchema = ConnectSchemas.columnsToConnectSchema(
        isKey ? logicalSpecSchema.key() : logicalSpecSchema.value());

    // Keys are unwrapped
    final Schema innerSchema = SerdeUtils.applySinglesUnwrapping(
        outerSchema,
        isKey
            ? SerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)
            : SerdeFeatures.of()
    );

    return new SpecJsonSchemaDeserializer(
        super.getDeserializer(schemaRegistryClient, isKey),
        innerSchema
    );
  }

  private static class SpecJsonSchemaDeserializer implements Deserializer<Object> {
    private final Deserializer<Object> internalDeserializer;
    private final Schema specSchema;

    SpecJsonSchemaDeserializer(
        final Deserializer<Object> internalDeserializer,
        final Schema specSchema
    ) {
      this.internalDeserializer = internalDeserializer;
      this.specSchema = specSchema;
    }

    @Override
    public Object deserialize(final String topic, final byte[] bytes) {
      final Object deserialized = internalDeserializer.deserialize(topic, bytes);
      if (!(deserialized instanceof Map)) {
        return deserialized;
      }

      final Map<String, Object> deserializedMap = (Map<String, Object>) deserialized;

      // This makes JSON_SR output compatible with old QTT tests that used to deserialize
      // JSON_SR with a JSON object mapper instead of the ConnectSerdeSupplier.
      // The ConnectSerdeSupplier is adding the missing fields with null values even if they're
      // not part of the logical schema. This was causing QTT historical plans because record
      // outputs now have the new missing fields.
      // This logic is removing those fields that do not match the logical schema.
      deserializedMap.entrySet().removeIf(entry ->
          specSchema.field(entry.getKey()) == null
              && specSchema.field(entry.getKey().toUpperCase()) == null
      );

      return deserialized;
    }
  }
}
