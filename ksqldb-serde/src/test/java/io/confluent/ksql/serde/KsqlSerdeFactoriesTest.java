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

import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.DELIMITED;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.serde.delimited.KsqlDelimitedSerdeFactory;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.serde.kafka.KafkaSerdeFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlSerdeFactoriesTest {

  private static final Class<SomeType> SOME_TYPE = SomeType.class;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Function<FormatInfo, KsqlSerdeFactory> factoryMethod;
  @Mock
  private FormatInfo formatInfo;
  @Mock
  private PersistenceSchema schema;
  @Mock
  private KsqlConfig config;
  @Mock
  private Supplier<SchemaRegistryClient> srClientFactory;
  @Mock
  private KsqlSerdeFactory ksqlSerdeFactory;
  @Mock
  private Serde<SomeType> serde;
  private KsqlSerdeFactories factory;

  @Before
  public void setUp() {
    factory = new KsqlSerdeFactories(factoryMethod);

    when(factoryMethod.apply(any())).thenReturn(ksqlSerdeFactory);
  }

  @Test
  public void shouldValidateOnValidate() {
    // When:
    factory.validate(formatInfo, schema);

    // Then:
    verify(ksqlSerdeFactory).validate(schema);
  }

  @Test
  public void shouldThrowOnValidateIfValidationFails() {
    // Given:
    doThrow(new RuntimeException("Boom!"))
        .when(ksqlSerdeFactory).validate(any());

    // Expect:
    expectedException.expectMessage("Boom!");

    // When:
    factory.validate(formatInfo, schema);
  }

  @Test
  public void shouldCreateFactory() {
    // When:
    factory.create(
        formatInfo,
        schema,
        config,
        srClientFactory,
        SOME_TYPE
    );

    // Then:
    verify(factoryMethod).apply(formatInfo);
  }

  @Test
  public void shouldValidateSerdeFactoryCanHandleSchema() {
    // When:
    factory.create(
        formatInfo,
        schema,
        config,
        srClientFactory,
        SOME_TYPE
    );

    // Then:
    verify(ksqlSerdeFactory).validate(schema);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void shouldCreateSerde() {
    // Given:
    when(ksqlSerdeFactory.createSerde(any(), any(), any(), any())).thenReturn((Serde)serde);

    // When:
    final Serde<SomeType> result = factory.create(
        formatInfo,
        schema,
        config,
        srClientFactory,
        SOME_TYPE
    );

    // Then:
    verify(ksqlSerdeFactory).createSerde(
        schema,
        config,
        srClientFactory,
        SOME_TYPE
    );

    assertThat(result, is(serde));
  }

  @Test
  public void shouldHandleAvro() {
    // When:
    final KsqlSerdeFactory result = KsqlSerdeFactories
        .create(FormatInfo.of(AVRO.name()));

    // Then:
    assertThat(result, instanceOf(KsqlAvroSerdeFactory.class));
  }

  @Test
  public void shouldHandleJson() {
    // When:
    final KsqlSerdeFactory result = KsqlSerdeFactories
        .create(FormatInfo.of(JSON.name()));

    // Then:
    assertThat(result, instanceOf(KsqlJsonSerdeFactory.class));
  }

  @Test
  public void shouldHandleDelimited() {
    // When:
    final KsqlSerdeFactory result = KsqlSerdeFactories
        .create(FormatInfo.of(DELIMITED.name()));

    // Then:
    assertThat(result, instanceOf(KsqlDelimitedSerdeFactory.class));
  }

  @Test
  public void shouldHandleKafka() {
    // When:
    final KsqlSerdeFactory result = KsqlSerdeFactories
        .create(FormatInfo.of(KAFKA.name()));

    // Then:
    assertThat(result, instanceOf(KafkaSerdeFactory.class));
  }

  private static final class SomeType {
  }
}