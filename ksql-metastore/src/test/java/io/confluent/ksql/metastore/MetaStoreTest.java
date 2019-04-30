/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metastore;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetaStoreTest {

  private MutableMetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
  }

  @Test
  public void testTopicMap() {
    final KsqlTopic ksqlTopic1 = new KsqlTopic("testTopic", "testTopicKafka", new KsqlJsonTopicSerDe(), false);
    metaStore.putTopic(ksqlTopic1);
    final KsqlTopic ksqlTopic2 = metaStore.getTopic("testTopic");
    Assert.assertNotNull(ksqlTopic2);

    // Check non-existent topic
    final KsqlTopic ksqlTopic3 = metaStore.getTopic("TESTTOPIC_");
    Assert.assertNull(ksqlTopic3);
  }

  @Test
  public void testStreamMap() {
    final DataSource<?> dataSource1 = metaStore.getSource("ORDERS");
    Assert.assertNotNull(dataSource1);
    assertThat(dataSource1.getDataSourceType(), is(DataSourceType.KSTREAM));

    // Check non-existent stream
    final DataSource<?> dataSource2 = metaStore.getSource("nonExistentStream");
    Assert.assertNull(dataSource2);
  }

  @Test
  public void testDelete() {
    final DataSource<?> dataSource1 = metaStore.getSource("ORDERS");
    final StructuredDataSource<?> dataSource2 = new KsqlStream<>(
        "sqlexpression", "testStream",
        dataSource1.getSchema(),
        dataSource1.getKeyField(),
        dataSource1.getTimestampExtractionPolicy(),
        dataSource1.getKsqlTopic(),
        Serdes::String);

    metaStore.putSource(dataSource2);
    final DataSource<?> dataSource3 = metaStore.getSource("testStream");
    Assert.assertNotNull(dataSource3);
    metaStore.deleteSource("testStream");
    final DataSource<?> dataSource4 = metaStore.getSource("testStream");
    Assert.assertNull(dataSource4);
  }

  @Test
  public void shouldGetSourcesForKafkaTopicWithSingleSource() {
    // When:
    final List<DataSource<?>> sources = metaStore.getSourcesForKafkaTopic("test2");

    // Then:
    assertThat(sources, hasSize(1));
    final DataSource<?> source = sources.get(0);
    assertThat(source, instanceOf(KsqlTable.class));
    assertThat(source.getDataSourceType(), equalTo(DataSourceType.KTABLE));
    assertThat(source.getKafkaTopicName(), equalTo("test2"));
  }

  @Test
  public void shouldGetSourcesForKafkaTopicWithMultipleSources() {
    // Given:
    final StructuredDataSource<?> mockSource = mock(StructuredDataSource.class);
    when(mockSource.getKafkaTopicName()).thenReturn("test1");
    when(mockSource.getName()).thenReturn("new source name");
    metaStore.putSource(mockSource);

    // When:
    final List<DataSource<?>> sources = metaStore.getSourcesForKafkaTopic("test1");

    // Then:
    assertThat(sources, hasSize(2));
    assertThat(sources.get(0).getKafkaTopicName(), equalTo("test1"));
    assertThat(sources.get(1).getKafkaTopicName(), equalTo("test1"));
  }

  @Test
  public void shouldGetSourcesForKafkaTopicWithNoSources() {
    // When:
    final List<DataSource<?>> sources = metaStore.getSourcesForKafkaTopic("not a topic name");

    // Then:
    assertThat(sources, hasSize(0));
  }
}