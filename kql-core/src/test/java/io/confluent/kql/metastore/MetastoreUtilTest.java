/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.metastore;

import org.junit.Assert;
import org.junit.Test;

import io.confluent.kql.serde.avro.KQLAvroTopicSerDe;
import io.confluent.kql.serde.json.KQLJsonTopicSerDe;

public class MetastoreUtilTest {

  @Test
  public void testMetastoreLoadingFromFile() throws Exception {

    MetaStore metaStore = new MetastoreUtil().loadMetaStoreFromJSONFile
        ("src/test/resources/TestCatalog.json");
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
    MetastoreUtil metastoreUtil = new MetastoreUtil();


  }

}
