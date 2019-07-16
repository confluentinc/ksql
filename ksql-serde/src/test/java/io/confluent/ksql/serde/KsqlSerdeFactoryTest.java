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

package io.confluent.ksql.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlSerdeFactoryTest {

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final PersistenceSchema SOME_SCHEMA = persistenceSchema(
      SchemaBuilder.struct()
          .field("f0", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("f1", Schema.OPTIONAL_INT32_SCHEMA)
          .build());

  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private SerializerFactory serializerFactory;
  @Mock
  private DeserializerFactory deserializerFactory;
  @Mock
  private Serializer<Object> serializer;
  @Mock
  private Deserializer<Object> deserializer;

  private TestSerdeFactory factory;

  @Before
  public void setUp() {
    when(serializerFactory.createSerializer(any(), any(), any()))
        .thenReturn(serializer);

    when(deserializerFactory.createDeserializer(any(), any(), any()))
        .thenReturn(deserializer);

    factory = new TestSerdeFactory(serializerFactory, deserializerFactory);
  }

  @Test
  public void shouldCreateSerializer() {
    // When:
    factory.createSerde(
        SOME_SCHEMA,
        KSQL_CONFIG,
        srClientFactory
    );

    // Then:
    verify(serializerFactory)
        .createSerializer(SOME_SCHEMA, KSQL_CONFIG, srClientFactory);
  }

  @Test
  public void shouldCreateDeserializer() {
    // When:
    factory.createSerde(
        SOME_SCHEMA,
        KSQL_CONFIG,
        srClientFactory
    );

    // Then:
    verify(deserializerFactory)
        .createDeserializer(SOME_SCHEMA, KSQL_CONFIG, srClientFactory);
  }

  @Test
  public void shouldReturnCreatedSerde() {
    // When:
    final Serde<Object> serde = factory.createSerde(
        SOME_SCHEMA,
        KSQL_CONFIG,
        srClientFactory
    );

    // Then:
    assertThat(serde.serializer(), is(serializer));
    assertThat(serde.deserializer(), is(deserializer));
  }

  private static PersistenceSchema persistenceSchema(final Schema connectSchema) {
    return PersistenceSchema.of((ConnectSchema) connectSchema);
  }

  private static final class TestSerdeFactory extends KsqlSerdeFactory {

    private final SerializerFactory serializerFactory;
    private final DeserializerFactory deserializerFactory;

    private TestSerdeFactory(
        final SerializerFactory serializerFactory,
        final DeserializerFactory deserializerFactory
    ) {
      super(Format.JSON);
      this.serializerFactory = serializerFactory;
      this.deserializerFactory = deserializerFactory;
    }

    @Override
    public void validate(final ConnectSchema schema) {
    }

    @Override
    protected Serializer<Object> createSerializer(
        final PersistenceSchema schema,
        final KsqlConfig ksqlConfig,
        final Supplier<SchemaRegistryClient> srClientFactory
    ) {
      return serializerFactory
          .createSerializer(schema, ksqlConfig, srClientFactory);
    }

    @Override
    protected Deserializer<Object> createDeserializer(
        final PersistenceSchema schema,
        final KsqlConfig ksqlConfig,
        final Supplier<SchemaRegistryClient> srClientFactory
    ) {
      return deserializerFactory
          .createDeserializer(schema, ksqlConfig, srClientFactory);
    }
  }

  private interface SerializerFactory {

    Serializer<Object> createSerializer(
        final PersistenceSchema schema,
        final KsqlConfig ksqlConfig,
        final Supplier<SchemaRegistryClient> srClientFactory
    );
  }

  private interface DeserializerFactory {

    Deserializer<Object> createDeserializer(
        final PersistenceSchema schema,
        final KsqlConfig ksqlConfig,
        final Supplier<SchemaRegistryClient> srClientFactory
    );
  }
}
