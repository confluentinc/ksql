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

package io.confluent.ksql.metastore;

import io.confluent.ksql.serde.avro.KsqlAvroTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetastoreUtilTest {

  private static final String TEST_RESOURCES_DIRECTORY = "src/test/resources/";

  @Test
  public void testMetastoreLoadingFromFile() throws Exception {

    MetaStore metaStore = new MetastoreUtil().loadMetaStoreFromJsonFile
        (TEST_RESOURCES_DIRECTORY + "TestCatalog.json");
    Assert.assertNotNull(metaStore.getTopic("ORDERS_TOPIC"));
    Assert.assertNotNull(metaStore.getTopic("USERS_TOPIC"));
    Assert.assertNotNull(metaStore.getTopic("ORDERS_TOPIC_AVRO"));
    Assert.assertNotNull(metaStore.getTopic("PAGEVIEW_TOPIC"));

    KsqlTopic ordersTopic = metaStore.getTopic("ORDERS_TOPIC");
    Assert.assertTrue(ordersTopic.getKsqlTopicSerDe() instanceof KsqlJsonTopicSerDe);
    Assert.assertTrue(ordersTopic.getTopicName().equalsIgnoreCase("ORDERS_TOPIC"));
    Assert.assertTrue(ordersTopic.getKafkaTopicName().equals("orders_kafka_topic"));

    KsqlTopic ordersAvroTopic = metaStore.getTopic("ORDERS_TOPIC_AVRO");
    Assert.assertTrue(ordersAvroTopic.getKsqlTopicSerDe() instanceof KsqlAvroTopicSerDe);
    Assert.assertTrue(ordersAvroTopic.getTopicName().equalsIgnoreCase("ORDERS_TOPIC_AVRO"));
    Assert.assertTrue(ordersAvroTopic.getKafkaTopicName().equals("orders_kafka_topic_avro"));

    KsqlTopic usersTopic = metaStore.getTopic("USERS_TOPIC");
    Assert.assertTrue(usersTopic.getKsqlTopicSerDe() instanceof KsqlJsonTopicSerDe);
    Assert.assertTrue(usersTopic.getTopicName().equalsIgnoreCase("USERS_TOPIC"));
    Assert.assertTrue(usersTopic.getKafkaTopicName().equals("users_kafka_topic_json"));

    StructuredDataSource orders = metaStore.getSource("ORDERS");
    Assert.assertTrue(orders instanceof KsqlStream);
    Assert.assertTrue(orders.dataSourceType == DataSource.DataSourceType.KSTREAM);
    Assert.assertTrue(orders.getSchema().fields().size() == 4);
    Assert.assertTrue(orders.getKeyField().name().equalsIgnoreCase("ordertime"));

    StructuredDataSource orders_avro = metaStore.getSource("ORDERS_AVRO");
    Assert.assertTrue(orders_avro instanceof KsqlStream);
    Assert.assertTrue(orders_avro.dataSourceType == DataSource.DataSourceType.KSTREAM);
    Assert.assertTrue(orders_avro.getSchema().fields().size() == 4);
    Assert.assertTrue(orders_avro.getKeyField().name().equalsIgnoreCase("ordertime"));

    StructuredDataSource users = metaStore.getSource("USERS");
    Assert.assertTrue(users instanceof KsqlTable);
    Assert.assertTrue(users.dataSourceType == DataSource.DataSourceType.KTABLE);
    Assert.assertTrue(users.getSchema().fields().size() == 4);
    Assert.assertTrue(users.getKeyField().name().equalsIgnoreCase("userid"));

  }

  @Test
  public void testReadWriteAvroSchema() {

  }

  // Without placing constraints on the output format of the exported catalog (which may change), it seems like the best
  // way to test the writing capabilities of the MetastoreUtil is to just create a metastore, export it, and then re-load
  // it, verifying that nothing has changed between the original and the re-loaded metastore.
  // Although this effectively tests both exporting and importing a catalog, we can assume that as long as
  // testMetastoreLoadingFromFile() succeeds, the only two possible causes of failure for this test are either the test
  // itself being invalid, or the MetastoreUtil failing to export metastores properly.
  @Test
  public void testMetastoreWritingToFile() throws Exception {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    File testCatalogFile = File.createTempFile("ExportedCatalog", ".json", new File(TEST_RESOURCES_DIRECTORY));

    MetaStore expectedMetaStore = new MetaStoreImpl();

    String topicName = "TOPIC_NAME";
    String kafkaTopicName = "KAFKA_TOPIC_NAME";
    KsqlTopic topic = new KsqlTopic(topicName, kafkaTopicName, new KsqlJsonTopicSerDe(null));
    expectedMetaStore.putTopic(topic);

    String tableSourceName = "TABLE_SOURCE";
    String tableKeyName = "TABLE_KEY";
    Schema tableSchema = SchemaBuilder.struct().field(tableKeyName, Schema.BOOLEAN_SCHEMA).name(tableSourceName).build();
    Field tableKey = tableSchema.field(tableKeyName);
    String tableStateStore = "STATE_STORE";
    expectedMetaStore.putSource(new KsqlTable(tableSourceName, tableSchema, tableKey, null, topic,
                                             tableStateStore, false));

    String streamSourceName = "STREAM_SOURCE";
    String streamKeyName = "STREAM_KEY";
    Schema streamSchema = SchemaBuilder.struct().field(streamKeyName, Schema.INT64_SCHEMA).name(streamSourceName).build();
    Field streamKey = streamSchema.field(streamKeyName);
    expectedMetaStore.putSource(new KsqlStream(streamSourceName, streamSchema, streamKey,
                                               null, topic));

    metastoreUtil.writeMetastoreToFile(testCatalogFile.getAbsolutePath(), expectedMetaStore);
    MetaStore testMetaStore = metastoreUtil.loadMetaStoreFromJsonFile(testCatalogFile.getAbsolutePath());

    Assert.assertNotNull(testMetaStore.getTopic(topicName));
    Assert.assertNotNull(testMetaStore.getSource(tableSourceName));
    Assert.assertNotNull(testMetaStore.getSource(streamSourceName));

    KsqlTopic testTopic = testMetaStore.getTopic(topicName);
    Assert.assertEquals(topicName, testTopic.getTopicName());
    Assert.assertEquals(kafkaTopicName, testTopic.getKafkaTopicName());
    Assert.assertTrue(testTopic.getKsqlTopicSerDe() instanceof KsqlJsonTopicSerDe);

    StructuredDataSource testTableSource = testMetaStore.getSource(tableSourceName);
    Assert.assertTrue(testTableSource instanceof KsqlTable);
    KsqlTable testTable = (KsqlTable) testTableSource;
    Assert.assertEquals(tableSchema, testTable.getSchema());
    Assert.assertEquals(tableKey, testTable.getKeyField());
    Assert.assertEquals(tableStateStore, testTable.getStateStoreName());

    StructuredDataSource testStreamSource = testMetaStore.getSource(streamSourceName);
    Assert.assertTrue(testStreamSource instanceof KsqlStream);
    KsqlStream testStream = (KsqlStream) testStreamSource;
    Assert.assertEquals(streamSchema, testStream.getSchema());
    Assert.assertEquals(streamKey, testStream.getKeyField());

    // Only delete the created file if the test is passed
    testCatalogFile.delete();
  }
  
  @Test
  public void testBuildAvroSchemaOne() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.map(schemaBuilder, schemaBuilder);
    schemaBuilder.field("", schemaBuilderTwo);

    try {
      metastoreUtil.buildAvroSchema(schemaBuilder, "timestamp");
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(MetastoreUtil.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testBuildAvroSchemaTwo() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.array(schemaBuilder);
    SchemaBuilder schemaBuilderThree = schemaBuilder.field("{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}", schemaBuilderTwo);

    try {
      metastoreUtil.buildAvroSchema(schemaBuilderThree, "$cpi_7vj9Q(:`'K7n:,");
      fail("Expecting exception: KsqlException");
    } catch (KsqlException e) {
      assertEquals(MetastoreUtil.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }

  @Test
  public void testBuildAvroSchemaAndBuildAvroSchemaOne() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.float64();
    schemaBuilder.field("{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}", schemaBuilderTwo);

    assertEquals("{\n\t\"namespace\": \"ksql\",\n\t\"name\": \"{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":" +
                    "[],\n\t\"schemas\" :[]\n}\",\n\t\"type\": \"record\",\n\t\"fields\": [\n\t\t{\"name\": \"{\n\t\"name\": " +
                    "\"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}\", \"type\": \"double\"}\n\t]\n}",
            metastoreUtil.buildAvroSchema(schemaBuilder, "{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}")
    );
  }

  @Test
  public void testBuildAvroSchemaAndBuildAvroSchemaTwo() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.int32();
    schemaBuilder.field("{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}", schemaBuilderTwo);

    assertEquals("{\n\t\"namespace\": \"ksql\",\n\t\"name\": \"{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":" +
                    "[],\n\t\"schemas\" :[]\n}\",\n\t\"type\": \"record\",\n\t\"fields\": [\n\t\t{\"name\": \"{\n\t\"name\": " +
                    "\"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}\", \"type\": \"int\"}\n\t]\n}",
            metastoreUtil.buildAvroSchema(schemaBuilder, "{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}")
    );
  }

  @Test
  public void testBuildAvroSchemaAndBuildAvroSchemaThree() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.bool();
    schemaBuilder.field("{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}", schemaBuilderTwo);

    assertEquals("{\n\t\"namespace\": \"ksql\",\n\t\"name\": \"{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":" +
                    "[],\n\t\"schemas\" :[]\n}\",\n\t\"type\": \"record\",\n\t\"fields\": [\n\t\t{\"name\": \"{\n\t\"name\": " +
                    "\"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}\", \"type\": \"boolean\"}\n\t]\n}",
            metastoreUtil.buildAvroSchema(schemaBuilder, "{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}")
    );
  }

  @Test
  public void testBuildAvroSchemaFour() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.string();
    schemaBuilder.field("{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}", schemaBuilderTwo);

    assertEquals("{\n\t\"namespace\": \"ksql\",\n\t\"name\": \"{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":" +
                    "[],\n\t\"schemas\" :[]\n}\",\n\t\"type\": \"record\",\n\t\"fields\": [\n\t\t{\"name\": \"{\n\t\"name\": " +
                    "\"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}\", \"type\": \"string\"}\n\t]\n}",
            metastoreUtil.buildAvroSchema(schemaBuilder, "{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}")
    );
  }

  @Test
  public void testBuildAvroSchemaFive() {
    MetastoreUtil metastoreUtil = new MetastoreUtil();
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    SchemaBuilder schemaBuilderTwo = SchemaBuilder.int64();
    schemaBuilder.field("\"int\"", schemaBuilderTwo);
    schemaBuilder.field("{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}", schemaBuilderTwo);

    assertEquals("{\n\t\"namespace\": \"ksql\",\n\t\"name\": \"{\n\t\"name\": " +
                    "\"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}\",\n\t\"type\": \"record\",\n\t\"fields\": " +
                    "[\n\t\t{\"name\": \"\"int\"\", \"type\": \"long\"},\n\t\t{\"name\": \"{\n\t\"name\": \"ksql_catalog\",\n" +
                    "\t\"topics\":[],\n\t\"schemas\" :[]\n}\", \"type\": \"long\"}\n\t]\n}",
            metastoreUtil.buildAvroSchema(schemaBuilder, "{\n\t\"name\": \"ksql_catalog\",\n\t\"topics\":[],\n\t\"schemas\" :[]\n}")
    );
  }

  @Test
  public void testWriteAvroSchemaFile() throws IOException {
    String testFileName = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "test_AvroSchemaFileName.test";
    MetastoreUtil metastoreUtil = new MetastoreUtil();

    try {
      metastoreUtil.writeAvroSchemaFile("Unsupported type: ", testFileName);

      assertTrue(Files.exists(Paths.get(testFileName)) );
    }
    finally {
      Files.delete(Paths.get(testFileName));
    }

  }
}