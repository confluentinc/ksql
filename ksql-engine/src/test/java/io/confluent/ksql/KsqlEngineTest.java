/*
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

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.easymock.EasyMock.mock;

public class KsqlEngineTest {

  private final KafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
      () -> schemaRegistryClient;
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final KsqlConfig ksqlConfig
      = new KsqlConfig(ImmutableMap.of(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
  private final KsqlEngine ksqlEngine = new KsqlEngine(
      topicClient,
      schemaRegistryClientFactory,
      new DefaultKafkaClientSupplier(),
      metaStore);

  @After
  public void closeEngine() {
    ksqlEngine.close();
  }

  @Test
  public void shouldCreatePersistentQueries() throws Exception {
    final List<QueryMetadata> queries
        = ksqlEngine.createQueries("create table bar as select * from test2;" +
        "create table foo as select * from test2;", ksqlConfig);

    assertThat(queries.size(), equalTo(2));
    final PersistentQueryMetadata queryOne = (PersistentQueryMetadata) queries.get(0);
    final PersistentQueryMetadata queryTwo = (PersistentQueryMetadata) queries.get(1);
    assertThat(queryOne.getEntity(), equalTo("BAR"));
    assertThat(queryTwo.getEntity(), equalTo("FOO"));
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailToCreateQueryIfSelectingFromNonExistentEntity() throws Exception {
    ksqlEngine.createQueries("select * from bar;", ksqlConfig);
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailWhenSyntaxIsInvalid() throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    final byte[] m = new byte[32];
    for (int i = 0; i < 32; i++) {
      m[i] = (byte)i;
    }
    final ByteBuffer bb = ByteBuffer.wrap(m);
    final String s = mapper.writeValueAsString(bb);
    ksqlEngine.createQueries("blah;", ksqlConfig);
  }

  @Test
  public void shouldUpdateReferentialIntegrityTableCorrectly() throws Exception {
    ksqlEngine.createQueries("create table bar as select * from test2;" +
                                   "create table foo as select * from test2;", ksqlConfig);
    final MetaStore metaStore = ksqlEngine.getMetaStore();
    assertThat(metaStore.getQueriesWithSource("TEST2"),
               equalTo(Utils.mkSet("CTAS_BAR_0", "CTAS_FOO_1")));
    assertThat(metaStore.getQueriesWithSink("BAR"), equalTo(Utils.mkSet("CTAS_BAR_0")));
    assertThat(metaStore.getQueriesWithSink("FOO"), equalTo(Utils.mkSet("CTAS_FOO_1")));
  }

  @Test
  public void shouldFailIfReferentialIntegrityIsViolated() {
    try {
      ksqlEngine.createQueries("create table bar as select * from test2;" +
                               "create table foo as select * from test2;", ksqlConfig);
      ksqlEngine.createQueries("drop table foo;", ksqlConfig);
      Assert.fail();
    } catch (final Exception e) {
      assertThat(e.getCause(), instanceOf(KsqlReferentialIntegrityException.class));
      assertThat(e.getMessage(), equalTo(
          "Exception while processing statements :Cannot drop FOO. \n"
          + "The following queries read from this source: []. \n"
          + "The following queries write into this source: [CTAS_FOO_1]. \n"
          + "You need to terminate them before dropping FOO."));
    }
  }

  @Test
  public void shouldFailDDLStatementIfTopicDoesNotExist() {
    final String ddlStatement = "CREATE STREAM S1_NOTEXIST (COL1 BIGINT, COL2 VARCHAR) "
                          + "WITH  (KAFKA_TOPIC = 'S1_NOTEXIST', VALUE_FORMAT = 'JSON');";
    try {
      final List<QueryMetadata> queries =
          ksqlEngine.buildMultipleQueries(ddlStatement.toString(), ksqlConfig, Collections.emptyMap());
      Assert.fail();
    } catch (final Exception e) {
      assertThat(e.getMessage(), equalTo("Kafka topic does not exist: S1_NOTEXIST"));
    }
  }

  @Test
  public void shouldDropTableIfAllReferencedQueriesTerminated() throws Exception {
    ksqlEngine.createQueries("create table bar as select * from test2;" +
                             "create table foo as select * from test2;", ksqlConfig);
    ksqlEngine.terminateQuery(new QueryId("CTAS_FOO_1"), true);
    ksqlEngine.createQueries("drop table foo;", ksqlConfig);
    assertThat(ksqlEngine.getMetaStore().getSource("foo"), nullValue());
  }

  @Test
  public void shouldEnforceTopicExistenceCorrectly() throws Exception {
    topicClient.createTopic("s1_topic", 1, (short) 1);
    final StringBuilder runScriptContent =
        new StringBuilder("CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
                          + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n");
    runScriptContent.append("CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
                            + "S1 GROUP BY COL1;\n");
    runScriptContent.append("CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
                            + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON');\n");
    final List<QueryMetadata> queries =
        ksqlEngine.buildMultipleQueries(runScriptContent.toString(), ksqlConfig, Collections.emptyMap());
    Assert.assertTrue(topicClient.isTopicExists("T1"));
  }

  @Test
  public void shouldNotEnforceTopicExistanceWhileParsing() throws Exception {
    final StringBuilder runScriptContent =
        new StringBuilder("CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
                          + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n");
    runScriptContent.append("CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
                            + "S1 GROUP BY COL1;\n");
    runScriptContent.append("CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
                            + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON');\n");

    final List<Pair<String, Statement>> parsedStatements = ksqlEngine.parseQueries(
        runScriptContent.toString(), metaStore.clone());

    assertThat(parsedStatements.size(), equalTo(3));

  }

  @Test
  public void shouldCleanupSchemaAndTopicForStream() throws Exception {
    ksqlEngine.buildMultipleQueries(
        "create stream bar with (value_format = 'avro') as select * from test1;"
        + "create stream foo as select * from test1;",
        ksqlConfig, Collections.emptyMap());
    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register("BAR-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
    ksqlEngine.terminateQuery(new QueryId("CSAS_BAR_0"), true);
    ksqlEngine.buildMultipleQueries("DROP STREAM bar DELETE TOPIC;", ksqlConfig, Collections.emptyMap());
    assertThat(topicClient.isTopicExists("BAR"), equalTo(false));
    assertThat(schemaRegistryClient.getAllSubjects().contains("BAR-value"), equalTo(false));
  }

  @Test
  public void shouldCleanupSchemaAndTopicForTable() throws Exception {
    ksqlEngine.buildMultipleQueries(
            "create table bar with (value_format = 'avro') as select * from test2;"
            + "create table foo as select * from test2;",
            ksqlConfig, Collections.emptyMap());
    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register("BAR-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
    ksqlEngine.terminateQuery(new QueryId("CTAS_BAR_0"), true);
    ksqlEngine.buildMultipleQueries("DROP TABLE bar DELETE TOPIC;", ksqlConfig, Collections.emptyMap());
    assertThat(topicClient.isTopicExists("BAR"), equalTo(false));
    assertThat(schemaRegistryClient.getAllSubjects().contains("BAR-value"), equalTo(false));
  }

  @Test
  public void shouldNotDeleteSchemaNorTopicForStream() throws Exception {
    ksqlEngine.buildMultipleQueries(
        "create stream bar with (value_format = 'avro') as select * from test1;"
        + "create stream foo as select * from test1;",
        ksqlConfig, Collections.emptyMap());
    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register("BAR-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
    ksqlEngine.terminateQuery(new QueryId("CSAS_BAR_0"), true);
    ksqlEngine.buildMultipleQueries("DROP STREAM bar;", ksqlConfig, Collections.emptyMap());
    assertThat(topicClient.isTopicExists("BAR"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
  }

  @Test
  public void shouldInferSchemaIfNotPresent() throws Exception {
    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("field").type().intType().noDefault()
        .endRecord();
    topicClient.createTopic("bar", 1, (short) 1);
    ksqlEngine.getSchemaRegistryClient().register("bar-value", schema);
    ksqlEngine.buildMultipleQueries(
        "create stream bar with (value_format='avro', kafka_topic='bar');",
        ksqlConfig,
        Collections.emptyMap());

    final StructuredDataSource source = ksqlEngine.getMetaStore().getSource("BAR");
    final org.apache.kafka.connect.data.Schema ksqlSchema = source.getSchema();
    assertThat(ksqlSchema.fields().size(), equalTo(3));
    assertThat(ksqlSchema.fields().get(2).name(), equalTo("FIELD"));
    assertThat(
        ksqlSchema.fields().get(2).schema(),
        equalTo(org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA));
    assertThat(source.getSqlExpression(), containsString("(FIELD INTEGER)"));
  }

  @Test
  public void shouldNotDeleteSchemaNorTopicForTable() throws Exception {
    ksqlEngine.buildMultipleQueries(
        "create table bar with (value_format = 'avro') as select * from test2;"
        + "create table foo as select * from test2;",
        ksqlConfig, Collections.emptyMap());
    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register("BAR-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
    ksqlEngine.terminateQuery(new QueryId("CTAS_BAR_0"), true);
    ksqlEngine.buildMultipleQueries("DROP TABLE bar;", ksqlConfig, Collections.emptyMap());
    assertThat(topicClient.isTopicExists("BAR"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
  }

  @Test
  public void shouldCleanUpInternalTopicSchemasFromSchemaRegistry() throws Exception {
    final List<QueryMetadata> queries
        = ksqlEngine.buildMultipleQueries(
        "create stream s1  with (value_format = 'avro') as select * from test1;"
        + "create table t1 as select col1, count(*) from s1 group by col1;",
        ksqlConfig, Collections.emptyMap());
    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();
    ksqlEngine.getSchemaRegistryClient().register
        ("_confluent-ksql-default_query_CTAS_T1_1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-changelog-value", schema);
    ksqlEngine.getSchemaRegistryClient().register
        ("_confluent-ksql-default_query_CTAS_T1_1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-repartition-value", schema);

    assertThat(schemaRegistryClient.getAllSubjects().contains
        ("_confluent-ksql-default_query_CTAS_T1_1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-changelog-value"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects().contains
        ("_confluent-ksql-default_query_CTAS_T1_1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-repartition-value"), equalTo(true));
    ksqlEngine.terminateQuery(new QueryId("CTAS_T1_1"), true);
    assertThat(schemaRegistryClient.getAllSubjects().contains
        ("_confluent-ksql-default_query_CTAS_T1_1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-changelog-value"), equalTo(false));
    assertThat(schemaRegistryClient.getAllSubjects().contains
        ("_confluent-ksql-default_query_CTAS_T1_1-KSTREAM-AGGREGATE-STATE-STORE-0000000006"
         + "-repartition-value"), equalTo(false));
  }

  @Test
  public void shouldCloseInternallyCreatedTopicClientOnClose() {
    // Given:
    final KafkaTopicClient topicClient = niceMock(KafkaTopicClient.class);
    topicClient.close();
    expectLastCall();
    replay(topicClient);
    final KsqlEngine ksqlEngine = new KsqlEngine(
        topicClient,
        schemaRegistryClientFactory,
        metaStore);

    // When:
    ksqlEngine.close();

    // Then:
    verify(topicClient);
  }

  @Test
  public void shouldUseSerdeSupplierToBuildQueries() {
    final KsqlTopicSerDe mockKsqlSerde = mock(KsqlTopicSerDe.class);
    final MetaStore metaStore =
        MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry(), () -> mockKsqlSerde);
    final KsqlEngine ksqlEngine = new KsqlEngine(
        topicClient,
        schemaRegistryClientFactory,
        new DefaultKafkaClientSupplier(),
        metaStore
    );

    expect(
        mockKsqlSerde.getGenericRowSerde(
            anyObject(org.apache.kafka.connect.data.Schema.class),
            anyObject(KsqlConfig.class),
            anyBoolean(),
            same(schemaRegistryClientFactory)))
        .andDelegateTo(new KsqlJsonTopicSerDe())
        .atLeastOnce();

    replay(mockKsqlSerde);

    ksqlEngine.createQueries("create table bar as select * from test2;", ksqlConfig);

    verify(mockKsqlSerde);
  }

  @Test
  public void shouldSetPropertyInRunScript() {
    final Map<String, Object> overriddenProperties = new HashMap<>();
    final List<QueryMetadata> queries
        = ksqlEngine.buildMultipleQueries(
        "SET 'auto.offset.reset' = 'earliest'; ",
        ksqlConfig, overriddenProperties);
    assertThat(overriddenProperties.get("auto.offset.reset"), equalTo("earliest"));

  }
}
