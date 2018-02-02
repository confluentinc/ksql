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

import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class KsqlEngineTest {

  private final KafkaTopicClient topicClient = mock(KafkaTopicClient.class);
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

  @Test(expected = ParseFailedException.class)
  public void shouldFailToCreateQueryIfSelectingFromNonExistentEntity() throws Exception {
    ksqlEngine.createQueries("select * from bar;");
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailWhenSyntaxIsInvalid() throws Exception {
    ksqlEngine.createQueries("blah;");
  }

  @Test
  public void shouldUpdateReferentialIntegrityTableCorrectly() throws Exception {
    ksqlEngine.createQueries("create table bar as select * from test2;" +
                                   "create table foo as select * from test2;");
    MetaStore metaStore = ksqlEngine.getMetaStore();
    Assert.assertTrue(metaStore.getSourceForQuery("TEST2").contains("CTAS_BAR"));
    Assert.assertTrue(metaStore.getSourceForQuery("TEST2").contains("CTAS_FOO"));
    assertThat(metaStore.getSinkForQuery("BAR"), equalTo(Utils.mkSet("CTAS_BAR")));
    assertThat(metaStore.getSinkForQuery("FOO"), equalTo(Utils.mkSet("CTAS_FOO")));
  }

  @Test
  public void shouldFailIfRererentialIntegrityIsViolated() {
    try {
      ksqlEngine.createQueries("create table bar as select * from test2;" +
                                     "create table foo as select * from test2;");
      ksqlEngine.createQueries("drop table foo;");
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), equalTo("Exception while processing statements :Cannot drop the data source. The following queries read from this source: [] and the following queries write into this source: [CTAS_FOO]. You need to terminate them before dropping this source."));
    }
  }

  @Test
  public void shouldDropTableIfAllReferencedQueriesTerminated() throws Exception {
    ksqlEngine.createQueries("create table bar as select * from test2;" +
                             "create table foo as select * from test2;");
    ksqlEngine.terminateQuery(new QueryId("CTAS_FOO"), true);
    ksqlEngine.createQueries("drop table foo;");
  }
}