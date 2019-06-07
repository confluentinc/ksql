/**
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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.serde.avro.AvroSchemaTranslator;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AvroUtilTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private String ordersAvroSchemaStr = "{"
                     + "\"namespace\": \"kql\","
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

  @Test
  public void shouldPassAvroCheck() throws Exception {
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    final SchemaMetadata schemaMetadata = new SchemaMetadata(1, 1, ordersAvroSchemaStr);
    expect(schemaRegistryClient.getLatestSchemaMetadata(anyString())).andReturn(schemaMetadata);
    replay(schemaRegistryClient);
    final AbstractStreamCreateStatement abstractStreamCreateStatement = getAbstractStreamCreateStatement
        ("CREATE STREAM S1 WITH (kafka_topic='s1_topic', "
                                     + "value_format='avro' );");
    final AbstractStreamCreateStatement newAbstractStreamCreateStatement = AvroUtil.checkAndSetAvroSchema(
        abstractStreamCreateStatement, new HashMap<>(), schemaRegistryClient);
    assertThat(
        newAbstractStreamCreateStatement.getElements(),
        equalTo(
            Arrays.asList(
                new TableElement("ORDERTIME", new PrimitiveType(Type.KsqlType.BIGINT)),
                new TableElement("ORDERID", new PrimitiveType(Type.KsqlType.BIGINT)),
                new TableElement("ITEMID", new PrimitiveType(Type.KsqlType.STRING)),
                new TableElement("ORDERUNITS", new PrimitiveType(Type.KsqlType.DOUBLE)),
                new TableElement("ARRAYCOL", new Array(new PrimitiveType(Type.KsqlType.DOUBLE))),
                new TableElement("MAPCOL", new Map(new PrimitiveType(Type.KsqlType.DOUBLE)))
            )
        )
    );
  }

  @Test
  public void shouldNotPassAvroCheckIfSchemaDoesNotExist() throws Exception {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Avro schema for message values on topic s1_topic does not exist in the Schema Registry.\n" +
            "Subject: s1_topic-value\n" +
            "\n" +
            "Possible causes include:\n" +
            "- The topic itself does not exist\n" +
            "\t-> Use SHOW TOPICS; to check\n" +
            "- Messages on the topic are not Avro serialized\n" +
            "\t-> Use PRINT 's1_topic' FROM BEGINNING; to verify\n" +
            "- Messages on the topic have not been serialized using the Confluent Schema Registry Avro serializer\n" +
            "\t-> See https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html\n" +
            "- The schema is registered on a different instance of the Schema Registry\n" +
            "\t-> Use the REST API to list available subjects\n\t" +
            "https://docs.confluent.io/current/schema-registry/docs/api.html#get--subjects\n");

    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    expect(schemaRegistryClient.getLatestSchemaMetadata(anyString()))
        .andThrow(new RestClientException("Not found", 404, 40401));
    replay(schemaRegistryClient);
    final AbstractStreamCreateStatement abstractStreamCreateStatement = getAbstractStreamCreateStatement
        ("CREATE STREAM S1 WITH "
            + "(kafka_topic='s1_topic', "
            + "value_format='avro' );");

    AvroUtil.checkAndSetAvroSchema(abstractStreamCreateStatement, new HashMap<>(), schemaRegistryClient);
  }

  @Test
  public void shouldThrowIfAvroSchemaNotCompatibleWithKsql() throws Exception {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Unable to verify if the Avro schema for topic s1_topic is compatible with KSQL.\n" +
            "Reason: No name in schema: {\"type\":\"record\"}\n" +
            "\n" +
            "Please see https://github.com/confluentinc/ksql/issues/ to see if this particular reason is already\n" +
            "known, and if not log a new issue. Please include the full Avro schema that you are trying to use.");

    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    final SchemaMetadata schemaMetadata = new SchemaMetadata(1, 1, "{\"type\": \"record\"}");
    expect(schemaRegistryClient.getLatestSchemaMetadata(anyString())).andReturn(schemaMetadata);
    replay(schemaRegistryClient);
    final AbstractStreamCreateStatement abstractStreamCreateStatement = getAbstractStreamCreateStatement
        ("CREATE STREAM S1 WITH "
            + "(kafka_topic='s1_topic', "
            + "value_format='avro' );");

    AvroUtil.checkAndSetAvroSchema(abstractStreamCreateStatement, new HashMap<>(), schemaRegistryClient);
  }


  private PersistentQueryMetadata buildStubPersistentQueryMetadata(final Schema resultSchema,
                                                                   final KsqlTopic resultTopic) {
    final PersistentQueryMetadata mockPersistentQueryMetadata = mock(PersistentQueryMetadata.class);
    expect(mockPersistentQueryMetadata.getResultSchema()).andStubReturn(resultSchema);
    expect(mockPersistentQueryMetadata.getResultTopic()).andStubReturn(resultTopic);
    expect(mockPersistentQueryMetadata.getResultTopicSerde()).andStubReturn(
        resultTopic.getKsqlTopicSerDe().getSerDe());
    replay(mockPersistentQueryMetadata);
    return mockPersistentQueryMetadata;
  }

  @Test
  public void shouldValidatePersistentQueryResultCorrectly()
      throws IOException, RestClientException {
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    final KsqlTopic resultTopic = new KsqlTopic("testTopic", "testTopic", new KsqlAvroTopicSerDe());
    final Schema resultSchema = AvroSchemaTranslator.toKsqlSchema(ordersAvroSchemaStr);
    final PersistentQueryMetadata persistentQueryMetadata = buildStubPersistentQueryMetadata(resultSchema, resultTopic);
    final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    final org.apache.avro.Schema avroSchema = parser.parse(ordersAvroSchemaStr);
    expect(schemaRegistryClient.testCompatibility(anyString(), EasyMock.isA(avroSchema.getClass())))
        .andReturn(true);
    replay(schemaRegistryClient);
    AvroUtil.validatePersistentQueryResults(persistentQueryMetadata, schemaRegistryClient);
  }

  @Test
  public void shouldFailForInvalidResultAvroSchema()
      throws IOException, RestClientException {
    final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    final KsqlTopic resultTopic = new KsqlTopic("testTopic", "testTopic", new KsqlAvroTopicSerDe
        ());
    final Schema resultSchema = AvroSchemaTranslator.toKsqlSchema(ordersAvroSchemaStr);
    final PersistentQueryMetadata persistentQueryMetadata = buildStubPersistentQueryMetadata(resultSchema, resultTopic);
    expect(schemaRegistryClient.testCompatibility(anyString(), anyObject())).andReturn(false);
    replay(schemaRegistryClient);
    try {
      AvroUtil.validatePersistentQueryResults(persistentQueryMetadata, schemaRegistryClient);
      fail();
    } catch (final Exception e) {
      assertThat("Incorrect exception message", "Cannot register avro schema for testTopic since "
                                                + "it is not valid for schema registry.", equalTo(e.getMessage()));
    }
  }


  private AbstractStreamCreateStatement getAbstractStreamCreateStatement(final String statementString) {
    final List<Statement> statementList = new KsqlParser().buildAst
        (statementString, new MetaStoreImpl(new InternalFunctionRegistry()));
    if (statementList.get(0) instanceof AbstractStreamCreateStatement) {
      return (AbstractStreamCreateStatement) statementList.get(0);
    }
    throw new KsqlException("Invalid statement." + statementString);
  }

}
