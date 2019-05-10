/*
 * Copyright 2018 Confluent Inc.
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

import static io.confluent.ksql.logging.processing.ProcessingLoggerUtil.join;

import com.google.errorprone.annotations.Immutable;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.KsqlSerdeFactory;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.serde.tls.ThreadLocalDeserializer;
import io.confluent.ksql.serde.tls.ThreadLocalSerializer;
import io.confluent.ksql.serde.util.SerdeUtils;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

@Immutable
public class KsqlAvroSerdeFactory extends KsqlSerdeFactory {

  private final String fullSchemaName;

  public KsqlAvroSerdeFactory(final String fullSchemaName) {
    super(Format.AVRO);
    this.fullSchemaName = Objects.requireNonNull(fullSchemaName, "fullSchemaName").trim();
    if (this.fullSchemaName.isEmpty()) {
      throw new IllegalArgumentException("the schema name cannot be empty");
    }
  }

  @Override
  public Serde<Struct> createSerde(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    final Serializer<Struct> genericRowSerializer = new ThreadLocalSerializer<>(
        () -> createSerializer(
            schema,
            ksqlConfig,
            schemaRegistryClientFactory
        )
    );

    final Deserializer<Struct> genericRowDeserializer = new ThreadLocalDeserializer<>(
        () -> createDeserializer(
            schema,
            ksqlConfig,
            schemaRegistryClientFactory,
            loggerNamePrefix,
            processingLogContext)
    );

    return Serdes.serdeFrom(genericRowSerializer, genericRowDeserializer);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final KsqlAvroSerdeFactory that = (KsqlAvroSerdeFactory) o;
    return Objects.equals(fullSchemaName, that.fullSchemaName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), fullSchemaName);
  }

  private KsqlConnectSerializer createSerializer(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory
  ) {
    final AvroDataTranslator translator = createAvroTranslator(schema, ksqlConfig);

    final AvroConverter avroConverter =
        getAvroConverter(schemaRegistryClientFactory.get(), ksqlConfig);

    return new KsqlConnectSerializer(translator.getConnectSchema(), translator, avroConverter);
  }

  private KsqlConnectDeserializer createDeserializer(
      final Schema schema,
      final KsqlConfig ksqlConfig,
      final Supplier<SchemaRegistryClient> schemaRegistryClientFactory,
      final String loggerNamePrefix,
      final ProcessingLogContext processingLogContext
  ) {
    if (schema.type() != Type.STRUCT) {
      throw new IllegalArgumentException("KSQL expects all top level schemas to be STRUCTs");
    }

    final AvroDataTranslator translator = createAvroTranslator(schema, ksqlConfig);

    final AvroConverter avroConverter =
        getAvroConverter(schemaRegistryClientFactory.get(), ksqlConfig);

    final ProcessingLogger logger = processingLogContext.getLoggerFactory().getLogger(
        join(loggerNamePrefix, SerdeUtils.DESERIALIZER_LOGGER_NAME));

    return new KsqlConnectDeserializer(avroConverter, translator, logger);
  }

  private AvroDataTranslator createAvroTranslator(
      final Schema schema,
      final KsqlConfig ksqlConfig
  ) {
    final boolean useNamedMaps = ksqlConfig.getBoolean(KsqlConfig.KSQL_USE_NAMED_AVRO_MAPS);

    return new AvroDataTranslator(schema, fullSchemaName, useNamedMaps);
  }

  private static AvroConverter getAvroConverter(
      final SchemaRegistryClient schemaRegistryClient,
      final KsqlConfig ksqlConfig
  ) {
    final AvroConverter avroConverter = new AvroConverter(schemaRegistryClient);

    final Map<String, Object> avroConfig = ksqlConfig
        .originalsWithPrefix(KsqlConfig.KSQL_SCHEMA_REGISTRY_PREFIX);

    avroConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        ksqlConfig.getString(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY));

    avroConfig.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, false);

    avroConverter.configure(avroConfig, false);
    return avroConverter;
  }
}
