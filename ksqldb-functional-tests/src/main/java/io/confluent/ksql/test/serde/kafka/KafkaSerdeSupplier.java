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

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.test.TestFrameworkException;
import io.confluent.ksql.test.serde.SerdeSupplier;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaSerdeSupplier implements SerdeSupplier<Object> {

  private final LogicalSchema schema;

  public KafkaSerdeSupplier(final LogicalSchema schema) {
    this.schema = requireNonNull(schema, "schema");
  }

  @Override
  public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
    return new RowSerializer();
  }

  @Override
  public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
    return new RowDeserializer();
  }

  private Serde<?> getSerde(final boolean isKey) {
    final List<Column> columns = isKey ? schema.key() : schema.value();
    if (columns.isEmpty()) {
      // Actual serde should be Serdes.Void(), but the test framework uses strings to allow tests
      // to pass in data that should be ignored:
      return Serdes.String();
    }

    if (columns.size() != 1) {
      throw new IllegalStateException("KAFKA format only supports single column schemas.");
    }

    return getSerde(columns.get(0).type());
  }

  private static Serde<?> getSerde(final SqlType sqlType) {
    switch (sqlType.baseType()) {
      case INTEGER:
        return Serdes.Integer();
      case BIGINT:
        return Serdes.Long();
      case DOUBLE:
        return Serdes.Double();
      case STRING:
        return Serdes.String();
      default:
        throw new IllegalStateException("Unsupported type for KAFKA format");
    }
  }

  private final class RowSerializer implements Serializer<Object> {

    private Serializer<Object> delegate;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      delegate = (Serializer)getSerde(isKey).serializer();
      delegate.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final Object value) {
      return delegate.serialize(topic, value);
    }
  }

  private final class RowDeserializer implements Deserializer<Object> {

    private Deserializer<?> delegate;
    private String type;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
      this.type = isKey ? "key" : "value";
      delegate = getSerde(isKey).deserializer();
      delegate.configure(configs, isKey);
    }

    @Override
    public Object deserialize(final String topic, final byte[] bytes) {
      try {
        return delegate.deserialize(topic, bytes);
      } catch (final Exception e) {
        throw new TestFrameworkException("Failed to deserialize " + type + ". "
            + e.getMessage(), e);
      }
    }
  }
}