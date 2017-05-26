/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.metastore;

import io.confluent.ksql.serde.avro.KSQLAvroTopicSerDe;
import io.confluent.ksql.serde.json.KSQLJsonTopicSerDe;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MetastoreUtilTest {

  private static final String TEST_RESOURCES_DIRECTORY = "src/test/resources/";

  @Test
  public void testMetastoreLoadingFromFile() throws Exception {

    MetaStore metaStore = new MetastoreUtil().loadMetaStoreFromJSONFile
        (TEST_RESOURCES_DIRECTORY + "TestCatalog.json");
    Assert.assertNotNull(metaStore.getTopic("ORDERS_TOPIC"));
    Assert.assertNotNull(metaStore.getTopic("USERS_TOPIC"));
    Assert.assertNotNull(metaStore.getTopic("ORDERS_TOPIC_AVRO"));
    Assert.assertNotNull(metaStore.getTopic("PAGEVIEW_TOPIC"));

    KSQLTopic ordersTopic = metaStore.getTopic("ORDERS_TOPIC");
    Assert.assertTrue(ordersTopic.getKsqlTopicSerDe() instanceof KSQLJsonTopicSerDe);
    Assert.assertTrue(ordersTopic.getTopicName().equalsIgnoreCase("ORDERS_TOPIC"));
    Assert.assertTrue(ordersTopic.getKafkaTopicName().equals("orders_kafka_topic"));

    KSQLTopic ordersAvroTopic = metaStore.getTopic("ORDERS_TOPIC_AVRO");
    Assert.assertTrue(ordersAvroTopic.getKsqlTopicSerDe() instanceof KSQLAvroTopicSerDe);
    Assert.assertTrue(ordersAvroTopic.getTopicName().equalsIgnoreCase("ORDERS_TOPIC_AVRO"));
    Assert.assertTrue(ordersAvroTopic.getKafkaTopicName().equals("orders_kafka_topic_avro"));

    KSQLTopic usersTopic = metaStore.getTopic("USERS_TOPIC");
    Assert.assertTrue(usersTopic.getKsqlTopicSerDe() instanceof KSQLJsonTopicSerDe);
    Assert.assertTrue(usersTopic.getTopicName().equalsIgnoreCase("USERS_TOPIC"));
    Assert.assertTrue(usersTopic.getKafkaTopicName().equals("users_kafka_topic_json"));

    StructuredDataSource orders = metaStore.getSource("ORDERS");
    Assert.assertTrue(orders instanceof KSQLStream);
    Assert.assertTrue(orders.dataSourceType == DataSource.DataSourceType.KSTREAM);
    Assert.assertTrue(orders.getSchema().fields().size() == 4);
    Assert.assertTrue(orders.getKeyField().name().equalsIgnoreCase("ordertime"));

    StructuredDataSource orders_avro = metaStore.getSource("ORDERS_AVRO");
    Assert.assertTrue(orders_avro instanceof KSQLStream);
    Assert.assertTrue(orders_avro.dataSourceType == DataSource.DataSourceType.KSTREAM);
    Assert.assertTrue(orders_avro.getSchema().fields().size() == 4);
    Assert.assertTrue(orders_avro.getKeyField().name().equalsIgnoreCase("ordertime"));

    StructuredDataSource users = metaStore.getSource("USERS");
    Assert.assertTrue(users instanceof KSQLTable);
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
    KSQLTopic topic = new KSQLTopic(topicName, kafkaTopicName, new KSQLJsonTopicSerDe(null));
    expectedMetaStore.putTopic(topic);

    String tableSourceName = "TABLE_SOURCE";
    String tableKeyName = "TABLE_KEY";
    Schema tableSchema = SchemaBuilder.struct().field(tableKeyName, Schema.BOOLEAN_SCHEMA).name(tableSourceName).build();
    Field tableKey = tableSchema.field(tableKeyName);
    String tableStateStore = "STATE_STORE";
    expectedMetaStore.putSource(new KSQLTable(tableSourceName, tableSchema, tableKey, null, topic,
                                             tableStateStore, false));

    String streamSourceName = "STREAM_SOURCE";
    String streamKeyName = "STREAM_KEY";
    Schema streamSchema = SchemaBuilder.struct().field(streamKeyName, Schema.INT64_SCHEMA).name(streamSourceName).build();
    Field streamKey = streamSchema.field(streamKeyName);
    expectedMetaStore.putSource(new KSQLStream(streamSourceName, streamSchema, streamKey,
                                               null, topic));

    metastoreUtil.writeMetastoreToFile(testCatalogFile.getAbsolutePath(), expectedMetaStore);
    MetaStore testMetaStore = metastoreUtil.loadMetaStoreFromJSONFile(testCatalogFile.getAbsolutePath());

    Assert.assertNotNull(testMetaStore.getTopic(topicName));
    Assert.assertNotNull(testMetaStore.getSource(tableSourceName));
    Assert.assertNotNull(testMetaStore.getSource(streamSourceName));

    KSQLTopic testTopic = testMetaStore.getTopic(topicName);
    Assert.assertEquals(topicName, testTopic.getTopicName());
    Assert.assertEquals(kafkaTopicName, testTopic.getKafkaTopicName());
    Assert.assertTrue(testTopic.getKsqlTopicSerDe() instanceof KSQLJsonTopicSerDe);

    StructuredDataSource testTableSource = testMetaStore.getSource(tableSourceName);
    Assert.assertTrue(testTableSource instanceof KSQLTable);
    KSQLTable testTable = (KSQLTable) testTableSource;
    Assert.assertEquals(tableSchema, testTable.getSchema());
    Assert.assertEquals(tableKey, testTable.getKeyField());
    Assert.assertEquals(tableStateStore, testTable.getStateStoreName());

    StructuredDataSource testStreamSource = testMetaStore.getSource(streamSourceName);
    Assert.assertTrue(testStreamSource instanceof KSQLStream);
    KSQLStream testStream = (KSQLStream) testStreamSource;
    Assert.assertEquals(streamSchema, testStream.getSchema());
    Assert.assertEquals(streamKey, testStream.getKeyField());

    // Only delete the created file if the test is passed
    testCatalogFile.delete();
  }
}