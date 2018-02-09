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

package io.confluent.ksql;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KsqlEngineTest {

  private final KafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private final SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore();
  private final KsqlEngine ksqlEngine = new KsqlEngine(
      new KsqlConfig(Collections.singletonMap("bootstrap.servers", "localhost:9092")),
      topicClient,
      schemaRegistryClient,
      metaStore);

  @After
  public void closeEngine() {
    ksqlEngine.close();
  }

  @Test
  public void shouldCreatePersistentQueries() throws Exception {
    final List<QueryMetadata> queries
        = ksqlEngine.createQueries("create table bar as select * from test2;" +
        "create table foo as select * from test2;");

    assertThat(queries.size(), equalTo(2));
    final PersistentQueryMetadata queryOne = (PersistentQueryMetadata) queries.get(0);
    final PersistentQueryMetadata queryTwo = (PersistentQueryMetadata) queries.get(1);
    assertThat(queryOne.getEntity(), equalTo("BAR"));
    assertThat(queryTwo.getEntity(), equalTo("FOO"));
  }


  @Test
  public void shouldAddCorrectDataSourceIntoMetastore() throws Exception {
    topicClient.createTopic("s1topic", 1, (short)1);
    final List<QueryMetadata> queries
        = ksqlEngine.createQueries("create stream s1 (\"Group\" bigint, col1 varchar) with "
                                   + "(kafka_topic = 's1topic', value_format = 'json');" +
                                   " create stream s2 as select col1 from s1;");
    StructuredDataSource s1_DataSource = ksqlEngine.getMetaStore().getSource("S1");
    StructuredDataSource s2_DataSource = ksqlEngine.getMetaStore().getSource("S2");

    Assert.assertTrue(!s1_DataSource.getQuotedFieldNames().isEmpty());
    Assert.assertTrue(s2_DataSource.getQuotedFieldNames().isEmpty());
    Assert.assertTrue(s1_DataSource.getQuotedFieldNames().contains("GROUP"));

  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailToCreateQueryIfSelectingFromNonExistentEntity() throws Exception {
    ksqlEngine.createQueries("select * from bar;");
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailWhenSyntaxIsInvalid() throws Exception {
    ksqlEngine.createQueries("blah;");
  }

}