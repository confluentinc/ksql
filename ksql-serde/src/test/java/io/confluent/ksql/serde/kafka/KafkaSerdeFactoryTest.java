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

package io.confluent.ksql.serde.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSerdeFactoryTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  private KafkaSerdeFactory factory;

  @Before
  public void setUp() {
    factory = new KafkaSerdeFactory();
  }

  @Test
  public void shouldThrowOnValidateIfMultipleFields() {
    // Given:
    final ConnectSchema schema = (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", Schema.OPTIONAL_INT32_SCHEMA)
        .field("f1", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "The 'KAFKA' format only supports a single field. Got: f0 INT, f1 BIGINT");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfBoolean() {
    // Given:
    final ConnectSchema schema = schemaWithFieldOfType(Schema.OPTIONAL_BOOLEAN_SCHEMA);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("The 'KAFKA' format does not support type 'BOOLEAN'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfDecimal() {
    // Given:
    final ConnectSchema schema = schemaWithFieldOfType(DecimalUtil.builder(1, 1));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("The 'KAFKA' format does not support type 'DECIMAL'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfArray() {
    // Given:
    final ConnectSchema schema = schemaWithFieldOfType(SchemaBuilder
        .array(Schema.OPTIONAL_STRING_SCHEMA)
        .build());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("The 'KAFKA' format does not support type 'ARRAY'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfMap() {
    // Given:
    final ConnectSchema schema = schemaWithFieldOfType(SchemaBuilder
        .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .build());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("The 'KAFKA' format does not support type 'MAP'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfStruct() {
    // Given:
    final ConnectSchema schema = schemaWithFieldOfType(SchemaBuilder
        .struct()
        .field("f0", Schema.OPTIONAL_STRING_SCHEMA)
        .build());

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException
        .expectMessage("The 'KAFKA' format does not support type 'STRUCT'");

    // When:
    factory.validate(schema);
  }

  @Test
  public void shouldHandleNulls() {
    shouldHandle(Schema.OPTIONAL_INT32_SCHEMA, null);
  }

  @Test
  public void shouldHandleInt() {
    shouldHandle(Schema.OPTIONAL_INT32_SCHEMA, 1);
    shouldHandle(Schema.OPTIONAL_INT32_SCHEMA, Integer.MIN_VALUE);
    shouldHandle(Schema.OPTIONAL_INT32_SCHEMA, Integer.MAX_VALUE);
  }

  @Test
  public void shouldHandleBigInt() {
    shouldHandle(Schema.OPTIONAL_INT64_SCHEMA, 1L);
    shouldHandle(Schema.OPTIONAL_INT64_SCHEMA, Long.MIN_VALUE);
    shouldHandle(Schema.OPTIONAL_INT64_SCHEMA, Long.MAX_VALUE);
  }

  @Test
  public void shouldHandleDouble() {
    shouldHandle(Schema.OPTIONAL_FLOAT64_SCHEMA, 1.1D);
    shouldHandle(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.MIN_VALUE);
    shouldHandle(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.MAX_VALUE);
    shouldHandle(Schema.OPTIONAL_FLOAT64_SCHEMA, Double.MIN_NORMAL);
  }

  @Test
  public void shouldHandleString() {
    shouldHandle(Schema.OPTIONAL_STRING_SCHEMA, "Yo!");
  }

  private void shouldHandle(final Schema fieldSchema, final Object value) {
    // Given:
    final PersistenceSchema schema =
        PersistenceSchema.of(schemaWithFieldOfType(fieldSchema));

    factory.validate(schema.getConnectSchema());
    final Serde<Object> serde = factory.createSerde(schema, ksqlConfig, srClientFactory);

    final Struct struct = new Struct(schema.getConnectSchema());
    struct.put("f0", value);

    // When:
    final byte[] bytes = serde.serializer().serialize("topic", struct);
    final Object result = serde.deserializer().deserialize("topic", bytes);

    // Then:
    assertThat(result, is(struct));
  }

  private static ConnectSchema schemaWithFieldOfType(final Schema schema) {
    return (ConnectSchema) SchemaBuilder
        .struct()
        .field("f0", schema)
        .build();
  }
}