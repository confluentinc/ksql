/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.metastore;


import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetastoreTest {

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KsqlTestUtil.getNewMetaStore();
  }

  @Test
  public void testTopicMap() {
    KsqlTopic ksqlTopic1 = new KsqlTopic("testTopic", "testTopicKafka", new KsqlJsonTopicSerDe(null));
    metaStore.putTopic(ksqlTopic1);
    KsqlTopic ksqlTopic2 = metaStore.getTopic("testTopic");
    Assert.assertNotNull(ksqlTopic2);

    // Check non-existent topic
    KsqlTopic ksqlTopic3 = metaStore.getTopic("TESTTOPIC_");
    Assert.assertNull(ksqlTopic3);
  }

  @Test
  public void testStreamMap() {
    StructuredDataSource structuredDataSource1 = metaStore.getSource("ORDERS");
    Assert.assertNotNull(structuredDataSource1);
    Assert.assertTrue(structuredDataSource1.dataSourceType == DataSource.DataSourceType.KSTREAM);

    // Check non-existent stream
    StructuredDataSource structuredDataSource2 = metaStore.getSource("nonExistentStream");
    Assert.assertNull(structuredDataSource2);
  }

  @Test
  public void testDelete() {
    StructuredDataSource structuredDataSource1 = metaStore.getSource("ORDERS");
    StructuredDataSource structuredDataSource2 = new KsqlStream("testStream",
                                                               structuredDataSource1.getSchema(),
                                                               structuredDataSource1.getKeyField(),
                                                               structuredDataSource1.getTimestampField(),
                                                               structuredDataSource1.getKsqlTopic());

    metaStore.putSource(structuredDataSource2);
    StructuredDataSource structuredDataSource3 = metaStore.getSource("testStream");
    Assert.assertNotNull(structuredDataSource3);
    metaStore.deleteSource("testStream");
    StructuredDataSource structuredDataSource4 = metaStore.getSource("testStream");
    Assert.assertNull(structuredDataSource4);
  }

}