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


import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetastoreTest {

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }


  @Test
  public void testTopicMap() {
    final KsqlTopic ksqlTopic1 = new KsqlTopic("testTopic", "testTopicKafka", new KsqlJsonTopicSerDe());
    metaStore.putTopic(ksqlTopic1);
    final KsqlTopic ksqlTopic2 = metaStore.getTopic("testTopic");
    Assert.assertNotNull(ksqlTopic2);

    // Check non-existent topic
    final KsqlTopic ksqlTopic3 = metaStore.getTopic("TESTTOPIC_");
    Assert.assertNull(ksqlTopic3);
  }

  @Test
  public void testStreamMap() {
    final StructuredDataSource structuredDataSource1 = metaStore.getSource("ORDERS");
    Assert.assertNotNull(structuredDataSource1);
    Assert.assertTrue(structuredDataSource1.dataSourceType == DataSource.DataSourceType.KSTREAM);

    // Check non-existent stream
    final StructuredDataSource structuredDataSource2 = metaStore.getSource("nonExistentStream");
    Assert.assertNull(structuredDataSource2);
  }

  @Test
  public void testDelete() {
    final StructuredDataSource structuredDataSource1 = metaStore.getSource("ORDERS");
    final StructuredDataSource structuredDataSource2 = new KsqlStream("sqlexpression", "testStream",
                                                               structuredDataSource1.getSchema(),
                                                               structuredDataSource1.getKeyField(),
                                                               structuredDataSource1.getTimestampExtractionPolicy(),
                                                               structuredDataSource1.getKsqlTopic());

    metaStore.putSource(structuredDataSource2);
    final StructuredDataSource structuredDataSource3 = metaStore.getSource("testStream");
    Assert.assertNotNull(structuredDataSource3);
    metaStore.deleteSource("testStream");
    final StructuredDataSource structuredDataSource4 = metaStore.getSource("testStream");
    Assert.assertNull(structuredDataSource4);
  }

}