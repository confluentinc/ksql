/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.metastore;


import io.confluent.kql.serde.json.KQLJsonTopicSerDe;
import io.confluent.kql.util.KQLTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetastoreTest {

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = KQLTestUtil.getNewMetaStore();
  }

  @Test
  public void testTopicMap() {
    KQLTopic kqlTopic = new KQLTopic("testTopic", "testTopicKafka", new KQLJsonTopicSerDe(null));
    metaStore.putTopic(kqlTopic);
    KQLTopic kqlTopic1 = metaStore.getTopic("testTopic");
    Assert.assertNotNull(kqlTopic1);

    // Check non-existant topic
    KQLTopic kqlTopic3 = metaStore.getTopic("TESTTOPIC_");
    Assert.assertNull(kqlTopic3);

  }

  @Test
  public void testStreamMap() {
    StructuredDataSource structuredDataSource1 = metaStore.getSource("ORDERS");
    Assert.assertNotNull(structuredDataSource1);
    Assert.assertTrue(structuredDataSource1.dataSourceType == DataSource.DataSourceType.KSTREAM);

    // Check non-existant stream
    StructuredDataSource structuredDataSource2 = metaStore.getSource("nonExistantStream");
    Assert.assertNull(structuredDataSource2);

  }

  @Test
  public void testDelete() {
    StructuredDataSource structuredDataSource1 = metaStore.getSource("ORDERS");
    StructuredDataSource structuredDataSource2 = new KQLStream("testStream",
                                                               structuredDataSource1.getSchema(),
                                                               structuredDataSource1.getKeyField
                                                                   (),
                                                               structuredDataSource1.getKqlTopic());
    metaStore.putSource(structuredDataSource2);
    StructuredDataSource structuredDataSource3 = metaStore.getSource("testStream");
    Assert.assertNotNull(structuredDataSource3);
    metaStore.deleteSource("testStream");
    StructuredDataSource structuredDataSource4 = metaStore.getSource("testStream");
    Assert.assertNull(structuredDataSource4);
  }

}
