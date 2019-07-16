/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.serde.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.test.serde.SerdeSupplier;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;

public class KafkaSerdeSupplier implements SerdeSupplier<Object> {

  private final Supplier<LogicalSchema> schemaSupplier;

  public KafkaSerdeSupplier(final Supplier<LogicalSchema> schemaSupplier) {
    this.schemaSupplier = Objects.requireNonNull(schemaSupplier, "schema");
  }

  @Override
  public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
    return new RowSerializer();
  }

  @Override
  public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    return new RowDeserializer();
  }

  private ConnectSchema getConnectSchema(final boolean isKey) {
    final LogicalSchema schema = schemaSupplier.get();
    final ConnectSchema connectSchema = isKey ? schema.keySchema() : schema.valueSchema();
    final List<Field> fields = connectSchema.fields();
    if (fields.isEmpty()) {
      throw new IllegalStateException("No fields in schema");
    }

    if (fields.size() != 1) {
      throw new IllegalStateException("KAFKA format only supports single field schemas.");
    }
    return connectSchema;
  }

  @SuppressWarnings("unchecked")
  private static Serde<Object> getSerde(
      final ConnectSchema connectSchema
  ) {
    switch (connectSchema.fields().get(0).schema().type()) {
      case INT32:
        return (Serde) Serdes.Integer();
      case INT64:
        return (Serde) Serdes.Long();
      case FLOAT64:
        return (Serde) Serdes.Double();
      case STRING:
        return (Serde) Serdes.String();
      default:
        throw new IllegalStateException("Unsupported type for KAFKA format");
    }
  }

  private final class RowSerializer implements Serializer<Object> {

    private Serializer<Object> delegate;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      final ConnectSchema connectSchema = getConnectSchema(isKey);
      delegate = getSerde(connectSchema).serializer();
      delegate.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Object value) {
      return delegate.serialize(topic, value);
    }
  }

  private final class RowDeserializer implements Deserializer<Object> {

    private Deserializer<Object> delegate;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      final ConnectSchema connectSchema = getConnectSchema(isKey);
      delegate = getSerde(connectSchema).deserializer();
      delegate.configure(configs, isKey);
    }

    @Override
    public Object deserialize(final String topic, final byte[] bytes) {
      return delegate.deserialize(topic, bytes);
    }
  }
}