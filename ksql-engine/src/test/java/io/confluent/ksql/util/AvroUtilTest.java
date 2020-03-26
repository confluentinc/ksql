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


package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.schema.ksql.KsqlSchema;
import io.confluent.ksql.schema.persistence.PersistenceSchema;
import io.confluent.ksql.schema.persistence.PersistenceSchemas;
import io.confluent.ksql.serde.avro.KsqlAvroSerdeFactory;
import io.confluent.ksql.serde.connect.ConnectSchemaTranslator;
import java.io.IOException;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroUtilTest {

  private static final String AVRO_SCHEMA_STRING = "{"
      + "\"namespace\": \"some.namespace\","
      + " \"name\": \"orders\","
      + " \"type\": \"record\","
      + " \"fields\": ["
      + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
      + "     {\"name\": \"orderid\",  \"type\": \"long\"},"
      + "     {\"name\": \"itemid\", \"type\": \"string\"},"
      + "     {\"name\": \"orderunits\", \"type\": \"double\"},"
      + "     {\"name\": \"arraycol\", \"type\": {\"type\": \"array\", \"items\": \"double\"}},"
      + "     {\"name\": \"mapcol\", \"type\": {\"type\": \"map\", \"values\": \"double\"}}"
      + " ]"
      + "}";

  private static final KsqlSchema RESULT_SCHEMA = toKsqlSchema(AVRO_SCHEMA_STRING);

  private static final PersistenceSchema VALUE_PERSISTENCE_SCHEMA = PersistenceSchema
      .of(RESULT_SCHEMA.getSchema());

  private static final PersistenceSchemas PERSISTENCE_SCHEMAS =
      PersistenceSchemas.of(VALUE_PERSISTENCE_SCHEMA);

  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(ImmutableMap.of());

  private static final KsqlTopic RESULT_TOPIC = new KsqlTopic(
      "registered-name",
      "actual-name",
      new KsqlAvroSerdeFactory(KsqlConstants.DEFAULT_AVRO_SCHEMA_FULL_NAME),
      false);

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private PersistentQueryMetadata persistentQuery;

  @Before
  public void setUp() {
    when(persistentQuery.getPersistenceSchemas()).thenReturn(PERSISTENCE_SCHEMAS);
    when(persistentQuery.getResultTopic()).thenReturn(RESULT_TOPIC);
    when(persistentQuery.getResultTopicFormat())
        .thenReturn(RESULT_TOPIC.getValueSerdeFactory().getFormat());
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSubject() throws Exception {
    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(eq(RESULT_TOPIC.getKafkaTopicName() + "-value"), any());
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSchema() throws Exception {
    // Given:
    final org.apache.avro.Schema expectedAvroSchema = SchemaUtil
        .buildAvroSchema(PERSISTENCE_SCHEMAS.valueSchema(), RESULT_TOPIC.getKsqlTopicName());

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(any(), eq(expectedAvroSchema));
  }

  @Test
  public void shouldReturnValidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any())).thenReturn(true);

    // When:
    final boolean result = AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldReturnInvalidEvolution() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any())).thenReturn(false);

    // When:
    final boolean result = AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    assertThat(result, is(false));
  }

  @Test
  public void shouldReturnValidEvolutionIfSubjectNotRegistered() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new RestClientException("Unknown subject", 404, 40401));

    // When:
    final boolean result = AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    assertThat(result, is(true));
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionSrException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new RestClientException("Unknown subject", 403, 40401));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> AvroUtil.isValidSchemaEvolution(persistentQuery, srClient)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not connect to Schema Registry service"));
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionIOException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new IOException("something"));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> AvroUtil.isValidSchemaEvolution(persistentQuery, srClient)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not check Schema compatibility"));
  }

  private static KsqlSchema toKsqlSchema(final String avroSchemaString) {
    final org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    final AvroData avroData = new AvroData(new AvroDataConfig(Collections.emptyMap()));
    return KsqlSchema.of(new ConnectSchemaTranslator()
        .toKsqlSchema(avroData.toConnectSchema(avroSchema)));
  }
}
