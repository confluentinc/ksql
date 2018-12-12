/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package io.confluent.ksql.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

  private static final org.apache.avro.Schema RESULT_AVRO_SCHEMA =
      new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_STRING);

  private static final Schema RESULT_SCHEMA = AvroUtil.toKsqlSchema(AVRO_SCHEMA_STRING);

  private static final KsqlTopic RESULT_TOPIC =
      new KsqlTopic("registered-name", "actual-name", new KsqlAvroTopicSerDe(), false);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SchemaRegistryClient srClient;
  @Mock
  private PersistentQueryMetadata persistentQuery;
  private AbstractStreamCreateStatement statement;

  @Before
  public void setUp() {
    when(persistentQuery.getResultSchema()).thenReturn(RESULT_SCHEMA);
    when(persistentQuery.getResultTopic()).thenReturn(RESULT_TOPIC);
    when(persistentQuery.getResultTopicSerde())
        .thenReturn(RESULT_TOPIC.getKsqlTopicSerDe().getSerDe());

    statement = createStreamCreateSql();
  }

  @Test
  public void shouldPassAvroCheck() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any())).thenReturn(
        new SchemaMetadata(1, 1, AVRO_SCHEMA_STRING));

    // When:
    final AbstractStreamCreateStatement result =
        AvroUtil.checkAndSetAvroSchema(statement, srClient);

    // Then:
    assertThat(result.getElements(), contains(
        new TableElement("ORDERTIME", new PrimitiveType(Type.KsqlType.BIGINT)),
        new TableElement("ORDERID", new PrimitiveType(Type.KsqlType.BIGINT)),
        new TableElement("ITEMID", new PrimitiveType(Type.KsqlType.STRING)),
        new TableElement("ORDERUNITS", new PrimitiveType(Type.KsqlType.DOUBLE)),
        new TableElement("ARRAYCOL", new Array(new PrimitiveType(Type.KsqlType.DOUBLE))),
        new TableElement("MAPCOL", new Map(new PrimitiveType(Type.KsqlType.DOUBLE)))
    ));
  }

  @Test
  public void shouldThrowIfAvroSchemaNotCompatibleWithKsql() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any())).thenReturn(
        new SchemaMetadata(1, 1, "{\"type\": \"record\"}"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Unable to verify if the Avro schema for topic s1_topic is compatible with KSQL.\n"
            + "Reason: No name in schema: {\"type\":\"record\"}\n\n"
            + "Please see https://github.com/confluentinc/ksql/issues/ to see if this particular reason is already\n"
            + "known, and if not log a new issue. Please include the full Avro schema that you are trying to use.");

    // When:
    AvroUtil.checkAndSetAvroSchema(statement, srClient);
  }

  @Test
  public void shouldNotPassAvroCheckIfSchemaDoesNotExist() throws Exception {
    // Given:
    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new RestClientException("Not found", 404, 40401));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Avro schema for message values on topic s1_topic does not exist in the Schema Registry.\n"
            + "Subject: s1_topic-value\n\n"
            + "Possible causes include:\n"
            + "- The topic itself does not exist\n"
            + "\t-> Use SHOW TOPICS; to check\n" +
            "- Messages on the topic are not Avro serialized\n"
            + "\t-> Use PRINT 's1_topic' FROM BEGINNING; to verify\n"
            + "- Messages on the topic have not been serialized using the Confluent Schema Registry Avro serializer\n"
            + "\t-> See https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html\n"
            + "- The schema is registered on a different instance of the Schema Registry\n"
            + "\t-> Use the REST API to list available subjects\n\t"
            + "https://docs.confluent.io/current/schema-registry/docs/api.html#get--subjects\n");

    // When:
    AvroUtil.checkAndSetAvroSchema(statement, srClient);
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSubject() throws Exception {
    // Given:
    when(persistentQuery.getResultTopic()).thenReturn(RESULT_TOPIC);

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);

    // Then:
    verify(srClient).testCompatibility(eq(RESULT_TOPIC.getKafkaTopicName() + "-value"), any());
  }

  @Test
  public void shouldValidateSchemaEvolutionWithCorrectSchema() throws Exception {
    // Given:
    when(persistentQuery.getResultSchema()).thenReturn(RESULT_SCHEMA);

    final org.apache.avro.Schema expectedAvroSchema = SchemaUtil
        .buildAvroSchema(RESULT_SCHEMA, RESULT_TOPIC.getName());

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

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not connect to Schema Registry service");

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);
  }

  @Test
  public void shouldThrowOnAnyOtherEvolutionIOException() throws Exception {
    // Given:
    when(srClient.testCompatibility(any(), any()))
        .thenThrow(new IOException("something"));

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not check Schema compatibility");

    // When:
    AvroUtil.isValidSchemaEvolution(persistentQuery, srClient);
  }

  private static AbstractStreamCreateStatement createStreamCreateSql() {
    final List<PreparedStatement<?>> statementList = new KsqlParser()
        .buildAst(
            "CREATE STREAM S1 WITH (kafka_topic='s1_topic', value_format='avro');",
            new MetaStoreImpl(new InternalFunctionRegistry())
        );

    return (AbstractStreamCreateStatement) statementList.get(0).getStatement();
  }
}
