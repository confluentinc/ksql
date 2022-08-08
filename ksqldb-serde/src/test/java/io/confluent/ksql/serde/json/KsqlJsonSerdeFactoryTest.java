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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.connect.KsqlConnectDeserializer;
import io.confluent.ksql.serde.connect.KsqlConnectSerializer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.function.Supplier;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlJsonSerdeFactoryTest {

  @Mock
  private KsqlConfig config;
  @Mock
  private KsqlJsonSerdeFactory jsonFactory;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;

  private Supplier<SchemaRegistryClient> srFactory;

  @Before
  public void setUp() {
    srFactory = () -> schemaRegistryClient;
    jsonFactory = new KsqlJsonSerdeFactory(new JsonSchemaProperties(ImmutableMap.of()));
  }

  @Test
  public void shouldUseNewJsonSchemaDeserializerOnJsonSrWhenJsonSchemaConverterIsEnabled() {
    // Given
    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder.string().build();
    when(config.getBoolean(KsqlConfig.KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED))
        .thenReturn(true);

    // When
    final Serde<String> serde =
        jsonFactory.createSerde(connectSchema, config, srFactory, String.class, false);

    // Then
    assertThat(serde.deserializer(), is(instanceOf(KsqlConnectDeserializer.class)));
  }

  @Test
  public void shouldUseOldJsonDeserializerOnJsonSrWhenJsonSchemaConverterIsDisabled() {
    // Given
    final ConnectSchema connectSchema = (ConnectSchema) SchemaBuilder.string().build();
    when(config.getBoolean(KsqlConfig.KSQL_JSON_SR_CONVERTER_DESERIALIZER_ENABLED))
        .thenReturn(false);

    // When
    final Serde<String> serde =
        jsonFactory.createSerde(connectSchema, config, srFactory, String.class, false);

    // Then
    assertThat(serde.deserializer(), is(instanceOf(KsqlJsonDeserializer.class)));
  }

  @Test
  public void shouldThrowOnMapWithNoneStringKeys() {
    // Given:
    final ConnectSchema schemaOfInvalidMap = (ConnectSchema) SchemaBuilder
        .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> jsonFactory.createSerde(schemaOfInvalidMap, config, srFactory, String.class, false)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }

  @Test
  public void shouldThrowOnNestedMapWithNoneStringKeys() {
    // Given
    final ConnectSchema schemaWithNestedInvalidMap = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", SchemaBuilder
            .map(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build())
        .build();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> jsonFactory.createSerde(schemaWithNestedInvalidMap, config, srFactory, String.class, false)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON only supports MAP types with STRING keys"));
  }
}