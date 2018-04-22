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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KsqlEngineTest {

  private final KafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
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
    assertThat(metaStore.getQueriesWithSource("TEST2"),
               equalTo(Utils.mkSet("CTAS_BAR", "CTAS_FOO")));
    assertThat(metaStore.getQueriesWithSink("BAR"), equalTo(Utils.mkSet("CTAS_BAR")));
    assertThat(metaStore.getQueriesWithSink("FOO"), equalTo(Utils.mkSet("CTAS_FOO")));
  }

  @Test
  public void shouldFailIfReferentialIntegrityIsViolated() {
    try {
      ksqlEngine.createQueries("create table bar as select * from test2;" +
                               "create table foo as select * from test2;");
      ksqlEngine.createQueries("drop table foo;");
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getCause(), instanceOf(KsqlReferentialIntegrityException.class));
      assertThat(e.getMessage(), equalTo(
          "Exception while processing statements :Cannot drop the data source. "
          + "The following queries read from this source: [] and the following queries write into "
          + "this source: [CTAS_FOO]. You need to terminate them before dropping this source."));
    }
  }

  @Test
  public void shouldFailDDLStatementIfTopicDoesNotExist() {
    String ddlStatement = "CREATE STREAM S1_NOTEXIST (COL1 BIGINT, COL2 VARCHAR) "
                          + "WITH  (KAFKA_TOPIC = 'S1_NOTEXIST', VALUE_FORMAT = 'JSON');";
    try {
      List<QueryMetadata> queries =
          ksqlEngine.buildMultipleQueries(ddlStatement.toString(), Collections.emptyMap());
      Assert.fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), equalTo("Kafka topic does not exist: S1_NOTEXIST"));
    }
  }

  @Test
  public void shouldDropTableIfAllReferencedQueriesTerminated() throws Exception {
    ksqlEngine.createQueries("create table bar as select * from test2;" +
                             "create table foo as select * from test2;");
    ksqlEngine.terminateQuery(new QueryId("CTAS_FOO"), true);
    ksqlEngine.createQueries("drop table foo;");
    assertThat(ksqlEngine.getMetaStore().getSource("foo"), nullValue());
  }

  @Test
  public void shouldEnforceTopicExistenceCorrectly() throws Exception {
    topicClient.createTopic("s1_topic", 1, (short) 1);
    StringBuilder runScriptContent =
        new StringBuilder("CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
                          + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n");
    runScriptContent.append("CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
                            + "S1 GROUP BY COL1;\n");
    runScriptContent.append("CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
                            + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON');\n");
    List<QueryMetadata> queries =
        ksqlEngine.buildMultipleQueries(runScriptContent.toString(), Collections.emptyMap());
    Assert.assertTrue(topicClient.isTopicExists("T1"));
  }

  @Test
  public void shouldNotEnforceTopicExistanceWhileParsing() throws Exception {
    StringBuilder runScriptContent =
        new StringBuilder("CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
                          + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n");
    runScriptContent.append("CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
                            + "S1 GROUP BY COL1;\n");
    runScriptContent.append("CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
                            + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON');\n");

    List<Pair<String, Statement>> parsedStatements = ksqlEngine.parseQueries(
        runScriptContent.toString(), Collections.emptyMap(), metaStore.clone());

    assertThat(parsedStatements.size(), equalTo(3));

  }

  @Test
  public void shouldCleanUpSchemaRegistry() throws Exception {
    ksqlEngine.buildMultipleQueries(
            "create table bar with (value_format = 'avro') as select * from test2;"
            + "create table foo as select * from test2;",
            Collections.emptyMap());
    Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register("BAR-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
    ksqlEngine.terminateQuery(new QueryId("CTAS_BAR"), true);
    ksqlEngine.buildMultipleQueries("DROP TABLE bar;", Collections.emptyMap());
    assertThat(schemaRegistryClient.getAllSubjects().contains("BAR-value"), equalTo(false));
  }

  @Test
  public void shouldCleanUpInternalTopicSchemasFromSchemaRegistry() throws Exception {
    final List<QueryMetadata> queries
        = ksqlEngine.buildMultipleQueries(
        "create stream s1  with (value_format = 'avro') as select * from test1;"
        + "create table t1 as select col1, count(*) from s1 group by col1;",
        Collections.emptyMap());
    Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register
        ("_confluent-ksql-default_query_CTAS_T1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-changelog-value", schema);
    ksqlEngine.getSchemaRegistryClient().register("_confluent-ksql-default_query_CTAS_T1-KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects().contains("_confluent-ksql-default_query_CTAS_T1-KSTREAM-AGGREGATE-STATE-STORE-0000000006-changelog-value"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects().contains("_confluent-ksql-default_query_CTAS_T1-KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition-value"), equalTo(true));
    ksqlEngine.terminateQuery(new QueryId("CTAS_T1"), true);
    assertThat(schemaRegistryClient.getAllSubjects().contains("_confluent-ksql-default_query_CTAS_T1-KSTREAM-AGGREGATE-STATE-STORE-0000000006-changelog-value"), equalTo(false));
    assertThat(schemaRegistryClient.getAllSubjects().contains("_confluent-ksql-default_query_CTAS_T1-KSTREAM-AGGREGATE-STATE-STORE-0000000006-repartition-value"), equalTo(false));
  }

}