/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.metastore;

import io.confluent.kql.serde.csv.KQLCsvTopicSerDe;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;

import java.io.File;

public class MetastoreUtilTest {

  private static final String TEST_RESOURCES_DIRECTORY = "src/test/resources/";

  @Test
  public void testMetastoreLoadingFromFile() throws Exception {

    MetaStore metaStore = new MetastoreUtil().loadMetastoreFromJSONFile
        (TEST_RESOURCES_DIRECTORY + "TestCatalog.json");
    Assert.assertNotNull(metaStore.getTopic("ORDERS_TOPIC"));
    Assert.assertNotNull(metaStore.getTopic("USERS_TOPIC"));
    Assert.assertNotNull(metaStore.getTopic("ORDERS_TOPIC_AVRO"));
    Assert.assertNotNull(metaStore.getTopic("PAGEVIEW_TOPIC"));

    KQLTopic ordersTopic = metaStore.getTopic("ORDERS_TOPIC");
    Assert.assertTrue(ordersTopic.getKqlTopicSerDe() instanceof KQLJsonTopicSerDe);
    Assert.assertTrue(ordersTopic.getTopicName().equalsIgnoreCase("ORDERS_TOPIC"));
    Assert.assertTrue(ordersTopic.getKafkaTopicName().equalsIgnoreCase("orders_kafka_topic"));

    KQLTopic ordersAvroTopic = metaStore.getTopic("ORDERS_TOPIC_AVRO");
    Assert.assertTrue(ordersAvroTopic.getKqlTopicSerDe() instanceof KQLAvroTopicSerDe);
    Assert.assertTrue(ordersAvroTopic.getTopicName().equalsIgnoreCase("ORDERS_TOPIC_AVRO"));
    Assert.assertTrue(ordersAvroTopic.getKafkaTopicName().equalsIgnoreCase("orders_kafka_topic_avro"));

    KQLTopic usersTopic = metaStore.getTopic("USERS_TOPIC");
    Assert.assertTrue(usersTopic.getKqlTopicSerDe() instanceof KQLJsonTopicSerDe);
    Assert.assertTrue(usersTopic.getTopicName().equalsIgnoreCase("USERS_TOPIC"));
    Assert.assertTrue(usersTopic.getKafkaTopicName().equalsIgnoreCase("users_kafka_topic_json"));

    StructuredDataSource orders = metaStore.getSource("orders");
    Assert.assertTrue(orders instanceof KQLStream);
    Assert.assertTrue(orders.dataSourceType == DataSource.DataSourceType.KSTREAM);
    Assert.assertTrue(orders.getSchema().fields().size() == 4);
    Assert.assertTrue(orders.getKeyField().name().equalsIgnoreCase("ORDERTIME"));

    StructuredDataSource orders_avro = metaStore.getSource("orders_avro");
    Assert.assertTrue(orders_avro instanceof KQLStream);
    Assert.assertTrue(orders_avro.dataSourceType == DataSource.DataSourceType.KSTREAM);
    Assert.assertTrue(orders_avro.getSchema().fields().size() == 4);
    Assert.assertTrue(orders_avro.getKeyField().name().equalsIgnoreCase("ORDERTIME"));

    StructuredDataSource users = metaStore.getSource("users");
    Assert.assertTrue(users instanceof KQLTable);
    Assert.assertTrue(users.dataSourceType == DataSource.DataSourceType.KTABLE);
    Assert.assertTrue(users.getSchema().fields().size() == 4);
    Assert.assertTrue(users.getKeyField().name().equalsIgnoreCase("USERID"));

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
    KQLTopic topic = new KQLTopic(topicName, kafkaTopicName, new KQLJsonTopicSerDe());
    expectedMetaStore.putTopic(topic);

    String tableSourceName = "TABLE_SOURCE";
    String tableKeyName = "TABLE_KEY";
    Schema tableSchema = SchemaBuilder.struct().field(tableKeyName, Schema.BOOLEAN_SCHEMA).name(tableSourceName).build();
    Field tableKey = tableSchema.field(tableKeyName);
    String tableStateStore = "STATE_STORE";
    expectedMetaStore.putSource(new KQLTable(tableSourceName, tableSchema, tableKey, topic, tableStateStore));

    String streamSourceName = "STREAM_SOURCE";
    String streamKeyName = "STREAM_KEY";
    Schema streamSchema = SchemaBuilder.struct().field(streamKeyName, Schema.INT64_SCHEMA).name(streamSourceName).build();
    Field streamKey = streamSchema.field(streamKeyName);
    expectedMetaStore.putSource(new KQLStream(streamSourceName, streamSchema, streamKey, topic));

    metastoreUtil.writeMetastoreToFile(testCatalogFile.getAbsolutePath(), expectedMetaStore);
    MetaStore testMetaStore = metastoreUtil.loadMetastoreFromJSONFile(testCatalogFile.getAbsolutePath());

    Assert.assertNotNull(testMetaStore.getTopic(topicName));
    Assert.assertNotNull(testMetaStore.getSource(tableSourceName));
    Assert.assertNotNull(testMetaStore.getSource(streamSourceName));

    KQLTopic testTopic = testMetaStore.getTopic(topicName);
    Assert.assertEquals(topicName, testTopic.getTopicName());
    Assert.assertEquals(kafkaTopicName, testTopic.getKafkaTopicName());
    Assert.assertTrue(testTopic.getKqlTopicSerDe() instanceof KQLJsonTopicSerDe);

    StructuredDataSource testTableSource = testMetaStore.getSource(tableSourceName);
    Assert.assertTrue(testTableSource instanceof KQLTable);
    KQLTable testTable = (KQLTable) testTableSource;
    Assert.assertEquals(tableSchema, testTable.getSchema());
    Assert.assertEquals(tableKey, testTable.getKeyField());
    Assert.assertEquals(tableStateStore, testTable.getStateStoreName());

    StructuredDataSource testStreamSource = testMetaStore.getSource(streamSourceName);
    Assert.assertTrue(testStreamSource instanceof KQLStream);
    KQLStream testStream = (KQLStream) testStreamSource;
    Assert.assertEquals(streamSchema, testStream.getSchema());
    Assert.assertEquals(streamKey, testStream.getKeyField());

    // Only delete the created file if the test is passed
    testCatalogFile.delete();
  }
}
