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

package io.confluent.ksql.test.serde;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * A general purpose serde supplier that works with any {@link io.confluent.ksql.serde.Format}
 * that can provide a {@link Converter} and supports Confluent Schema Registry
 * integration.
 */
public abstract class ConnectSerdeSupplier<T extends ParsedSchema>
    implements SerdeSupplier<Object> {

  private final Function<SchemaRegistryClient, Converter> converterFactory;

  protected ConnectSerdeSupplier(
      final Function<SchemaRegistryClient, Converter> converterFactory
  ) {
    this.converterFactory = Objects.requireNonNull(converterFactory, "converterFactory");
  }

  @Override
  public Serializer<Object> getSerializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new SpecSerializer(schemaRegistryClient, isKey);
  }

  @Override
  public Deserializer<Object> getDeserializer(
      final SchemaRegistryClient schemaRegistryClient,
      final boolean isKey
  ) {
    return new SpecDeserializer(schemaRegistryClient, isKey);
  }

  protected abstract Schema fromParsedSchema(T schema);

  private final class SpecSerializer implements Serializer<Object> {

    private final SchemaRegistryClient srClient;
    private final Converter converter;
    private final boolean isKey;

    SpecSerializer(final SchemaRegistryClient srClient, final boolean isKey) {
      this.srClient = Objects.requireNonNull(srClient, "srClient");
      this.converter = converterFactory.apply(srClient);
      converter.configure(
          ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "foo"),
          isKey
      );
      this.isKey = isKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] serialize(final String topic, final Object spec) {
      if (spec == null) {
        return null;
      }

      final T schema;
      final int id;
      try {
        final String subject = KsqlConstants.getSRSubject(topic, isKey);
        id = srClient.getLatestSchemaMetadata(subject).getId();
        schema = (T) srClient.getSchemaBySubjectAndId(subject, id);
      } catch (Exception e) {
        throw new KsqlException(e);
      }

      final Schema connectSchema = fromParsedSchema(schema);
      return converter.fromConnectData(
          topic,
          connectSchema,
          SpecToConnectConverter.specToConnect(spec, connectSchema)
      );
    }
  }

  private class SpecDeserializer implements Deserializer<Object> {

    private final Converter converter;

    SpecDeserializer(final SchemaRegistryClient srClient, final boolean isKey) {
      this.converter = converterFactory.apply(srClient);
      converter.configure(
          ImmutableMap.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "foo"),
          isKey
      );
    }

    @Override
    public Object deserialize(final String topic, final byte[] bytes) {
      if (bytes == null) {
        return null;
      }

      final SchemaAndValue schemaAndValue = converter.toConnectData(topic, bytes);
      return SpecToConnectConverter.connectToSpec(schemaAndValue.value(),
          schemaAndValue.schema(),
          false);
    }
  }
}
