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

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.fail;

public class AvroUtilTest {

  String ordersAveroSchemaStr = "{"
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
    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
    expect(schemaMetadata.getSchema()).andReturn(ordersAveroSchemaStr);
    expect(schemaMetadata.getId()).andReturn(1);
    expect(schemaRegistryClient.getLatestSchemaMetadata(anyString())).andReturn(schemaMetadata);
    replay(schemaMetadata);
    replay(schemaRegistryClient);
    AbstractStreamCreateStatement abstractStreamCreateStatement = getAbstractStreamCreateStatement
        ("CREATE STREAM S1 WITH "
                                                                   + "(kafka_topic='s1_topic', "
                                     + "value_format='avro' );");
    Pair<AbstractStreamCreateStatement, String> checkResult = AvroUtil.checkAndSetAvroSchema
        (abstractStreamCreateStatement, new HashMap<>(), schemaRegistryClient);
    AbstractStreamCreateStatement newAbstractStreamCreateStatement = checkResult.getLeft();
    assertThat("Columns were not added correctly from Avro schema.",
               newAbstractStreamCreateStatement.getElements().size(), equalTo(6));
    assertThat("Invalid column type mapping. ", newAbstractStreamCreateStatement.getElements()
        .get(2).getType(), equalTo("VARCHAR"));
    assertThat("Invalid column type mapping. ", newAbstractStreamCreateStatement.getElements()
        .get(5).getType(), equalTo("MAP<VARCHAR,DOUBLE>"));

    assertThat("Schema ID was not set correctly.", newAbstractStreamCreateStatement
        .getProperties().get(KsqlConstants.AVRO_SCHEMA_ID).toString(), equalTo("'1'"));
  }

  @Test
  public void shouldNotPassAvroCheckIfSchemaDoesNotExist() throws Exception {
    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
    expect(schemaMetadata.getSchema()).andReturn(null);
    expect(schemaMetadata.getId()).andReturn(1);
    expect(schemaRegistryClient.getLatestSchemaMetadata(anyString())).andReturn(schemaMetadata);
    replay(schemaMetadata);
    replay(schemaRegistryClient);
    AbstractStreamCreateStatement abstractStreamCreateStatement = getAbstractStreamCreateStatement
        ("CREATE STREAM S1 WITH "
         + "(kafka_topic='s1_topic', "
         + "value_format='avro' );");
    try {
      Pair<AbstractStreamCreateStatement, String> checkResult = AvroUtil.checkAndSetAvroSchema
          (abstractStreamCreateStatement, new HashMap<>(), schemaRegistryClient);
    } catch (Exception e) {
      assertThat("Expected different message message.", e.getMessage(), equalTo(" Could not "
                                                                               + "fetch the AVRO schema "
                                                          + "from schema "
                                              + "registry. null "));
      return;
    }
    fail();
  }

  @Test
  public void shouldValidatePersistentQueryResultCorrectly()
      throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    KsqlTopic resultTopic = new KsqlTopic("testTopic", "testTopic", new KsqlAvroTopicSerDe
        (ordersAveroSchemaStr));
    Schema resultSchema = SerDeUtil.getSchemaFromAvro(ordersAveroSchemaStr);
    PersistentQueryMetadata persistentQueryMetadata = new PersistentQueryMetadata("", null, null,
                                                                                  "", null,
                                                                                  DataSource.DataSourceType.KSTREAM, "", mock(KafkaTopicClient.class), new KsqlConfig(Collections.EMPTY_MAP), resultSchema, resultTopic);
    expect(schemaRegistryClient.testCompatibility(anyString(), anyObject())).andReturn(true);
    replay(schemaRegistryClient);
    AvroUtil.validatePersistantQueryResults(persistentQueryMetadata, schemaRegistryClient);
  }

  @Test
  public void shouldFailForInvalidResultAvroSchema()
      throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    KsqlTopic resultTopic = new KsqlTopic("testTopic", "testTopic", new KsqlAvroTopicSerDe
        (ordersAveroSchemaStr));
    Schema resultSchema = SerDeUtil.getSchemaFromAvro(ordersAveroSchemaStr);
    PersistentQueryMetadata persistentQueryMetadata = new PersistentQueryMetadata("", null, null,
                                                                                  "", null,
                                                                                  DataSource.DataSourceType.KSTREAM, "", mock(KafkaTopicClient.class), new KsqlConfig(Collections.EMPTY_MAP), resultSchema, resultTopic);
    expect(schemaRegistryClient.testCompatibility(anyString(), anyObject())).andReturn(false);
    replay(schemaRegistryClient);
    try {
      AvroUtil.validatePersistantQueryResults(persistentQueryMetadata, schemaRegistryClient);
    } catch (Exception e) {
      assertThat("Incorrect exception message", "Cannot register avro schema for testTopic since "
                                                + "it is not valid for schema registry.", equalTo
          (e.getMessage()));
      return;
    }
    fail();
  }

  private AbstractStreamCreateStatement getAbstractStreamCreateStatement(String statementString) {
    List<Statement> statementList = new KsqlParser().buildAst
        (statementString, new MetaStoreImpl());
    if (statementList.get(0) instanceof AbstractStreamCreateStatement) {
      return (AbstractStreamCreateStatement) statementList.get(0);
    }
    return null;
  }

}
