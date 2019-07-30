/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import static io.confluent.ksql.util.KsqlExceptionMatcher.rawMessage;
import static io.confluent.ksql.util.KsqlExceptionMatcher.statementText;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "SameParameterValue"})
@RunWith(MockitoJUnitRunner.class)
public class KsqlEngineTest {

  private static final KsqlConfig KSQL_CONFIG = KsqlConfigTestUtil.create("what-eva");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private MutableMetaStore metaStore;
  @Spy
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
      () -> schemaRegistryClient;

  private KsqlEngine ksqlEngine;
  private ServiceContext serviceContext;
  @Spy
  private final FakeKafkaTopicClient topicClient = new FakeKafkaTopicClient();
  private KsqlExecutionContext sandbox;

  @Before
  public void setUp() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

    serviceContext = TestServiceContext.create(
        topicClient,
        schemaRegistryClientFactory
    );

    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore
    );

    sandbox = ksqlEngine.createSandbox(serviceContext);
  }

  @After
  public void closeEngine() {
    ksqlEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldCreatePersistentQueries() {
    // When:
    final List<QueryMetadata> queries
        = KsqlEngineTestUtil.execute(ksqlEngine, "create table bar as select * from test2;" +
        "create table foo as select * from test2;", KSQL_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(queries, hasSize(2));
    assertThat(queries.get(0), is(instanceOf(PersistentQueryMetadata.class)));
    assertThat(queries.get(1), is(instanceOf(PersistentQueryMetadata.class)));
    assertThat(((PersistentQueryMetadata) queries.get(0)).getSinkName(), is("BAR"));
    assertThat(((PersistentQueryMetadata) queries.get(1)).getSinkName(), is("FOO"));
  }

  @Test
  public void shouldThrowOnTerminateAsNotExecutable() {
    // Given:
    final PersistentQueryMetadata query = (PersistentQueryMetadata) KsqlEngineTestUtil
        .execute(ksqlEngine,
            "create table bar as select * from test2;", KSQL_CONFIG, Collections.emptyMap()).get(0);

    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is("Statement not executable")));
    expectedException.expect(statementText(is("TERMINATE CTAS_BAR_0;")));

    // When:
    KsqlEngineTestUtil.execute(
        ksqlEngine, "TERMINATE " + query.getQueryId() + ";", KSQL_CONFIG, Collections.emptyMap());
  }

  @Test
  public void shouldExecuteInsertIntoStreamOnSandBox() {
    // Given:
    final List<ParsedStatement> statements = parse(
        "create stream bar as select * from orders;"
            + "insert into bar select * from orders;"
    );

    givenStatementAlreadyExecuted(statements.get(0));

    // When:
    final ExecuteResult result = sandbox
        .execute(ConfiguredStatement.of(
            sandbox.prepare(statements.get(1)),
            Collections.emptyMap(),
            KSQL_CONFIG));

    // Then:
    assertThat(result.getQuery(), is(not(Optional.empty())));
  }

  @Test
  public void shouldThrowWhenExecutingInsertIntoTable() {
    KsqlEngineTestUtil.execute(
        ksqlEngine, "create table bar as select * from test2;", KSQL_CONFIG,
        Collections.emptyMap());

    final ParsedStatement parsed = ksqlEngine.parse("insert into bar select * from test2;").get(0);

    expectedException.expect(ParseFailedException.class);
    expectedException.expect(rawMessage(containsString(
        "INSERT INTO can only be used to insert into a stream. BAR is a table.")));
    expectedException.expect(statementText(is("insert into bar select * from test2;")));

    // When:
    prepare(parsed);
  }

  @Test
  public void shouldExecuteInsertIntoStream() {
    // Given:
    KsqlEngineTestUtil.execute(
        ksqlEngine, "create stream bar as select * from orders;", KSQL_CONFIG,
        Collections.emptyMap());

    // When:
    final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(
        ksqlEngine, "insert into bar select * from orders;", KSQL_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(queries, hasSize(1));
  }

  @Test
  public void shouldMaintainOrderOfReturnedQueries() {
    // When:
    final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(ksqlEngine,
        "create stream foo as select * from orders;"
            + "create stream bar as select * from orders;",
        KSQL_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(queries, hasSize(2));
    assertThat(queries.get(0).getStatementString(), containsString("CREATE STREAM FOO"));
    assertThat(queries.get(1).getStatementString(), containsString("CREATE STREAM BAR"));
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailToCreateQueryIfSelectingFromNonExistentEntity() {
    KsqlEngineTestUtil
        .execute(ksqlEngine, "select * from bar;", KSQL_CONFIG, Collections.emptyMap());
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailWhenSyntaxIsInvalid() {
    KsqlEngineTestUtil.execute(ksqlEngine, "blah;", KSQL_CONFIG, Collections.emptyMap());
  }

  @Test
  public void shouldUpdateReferentialIntegrityTableCorrectly() {
    KsqlEngineTestUtil.execute(ksqlEngine, "create table bar as select * from test2;" +
        "create table foo as select * from test2;", KSQL_CONFIG, Collections
        .emptyMap());

    assertThat(metaStore.getQueriesWithSource("TEST2"),
        equalTo(Utils.mkSet("CTAS_BAR_0", "CTAS_FOO_1")));
    assertThat(metaStore.getQueriesWithSink("BAR"), equalTo(Utils.mkSet("CTAS_BAR_0")));
    assertThat(metaStore.getQueriesWithSink("FOO"), equalTo(Utils.mkSet("CTAS_FOO_1")));
  }

  @Test
  public void shouldFailIfReferentialIntegrityIsViolated() {
    // Given:
    KsqlEngineTestUtil.execute(ksqlEngine, "create table bar as select * from test2;" +
            "create table foo as select * from test2;",
        KSQL_CONFIG, Collections.emptyMap());

    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Cannot drop FOO.\n"
            + "The following queries read from this source: [].\n"
            + "The following queries write into this source: [CTAS_FOO_1].\n"
            + "You need to terminate them before dropping FOO.")));
    expectedException.expect(statementText(is("drop table foo;")));

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine, "drop table foo;", KSQL_CONFIG, Collections.emptyMap());
  }

  @Test
  public void shouldFailDDLStatementIfTopicDoesNotExist() {
    // Given:
    final ParsedStatement stmt = parse(
        "CREATE STREAM S1_NOTEXIST (COL1 BIGINT, COL2 VARCHAR) "
            + "WITH  (KAFKA_TOPIC = 'S1_NOTEXIST', VALUE_FORMAT = 'JSON');").get(0);

    final PreparedStatement<?> prepared = prepare(stmt);

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Kafka topic does not exist: S1_NOTEXIST");

    // When:
    sandbox.execute(ConfiguredStatement.of(prepared,  Collections.emptyMap(), KSQL_CONFIG));
  }

  @Test
  public void shouldDropTableIfAllReferencedQueriesTerminated() {
    // Given:
    final QueryMetadata secondQuery = KsqlEngineTestUtil.execute(ksqlEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KSQL_CONFIG, Collections.emptyMap()).get(1);

    secondQuery.close();

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine, "drop table foo;", KSQL_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(metaStore.getSource("foo"), nullValue());
  }

  @Test
  public void shouldNotEnforceTopicExistenceWhileParsing() {
    final String runScriptContent = "CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
        + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n"
        + "CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
        + "S1 GROUP BY COL1;\n"
        + "CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
        + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON');\n";

    final List<?> parsedStatements = ksqlEngine.parse(runScriptContent);

    assertThat(parsedStatements.size(), equalTo(3));
  }

  @Test
  public void shouldThrowFromSandBoxOnPrepareIfSourceTopicDoesNotExist() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM S1 (COL1 BIGINT) "
            + "WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON');").get(0));

    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Kafka topic does not exist: i_do_not_exist")));
    expectedException.expect(statementText(is(
        "CREATE STREAM S1 (COL1 BIGINT)"
            + " WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON');")));

    // When:
    sandbox.execute(ConfiguredStatement.of(statement, new HashMap<>(), KSQL_CONFIG));
  }

  @Test
  public void shouldThrowFromExecuteIfSourceTopicDoesNotExist() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM S1 (COL1 BIGINT) "
            + "WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON');").get(0));

    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is("Kafka topic does not exist: i_do_not_exist")));

    // When:
    ksqlEngine.execute(ConfiguredStatement.of(statement, new HashMap<>(), KSQL_CONFIG));
  }

  @Test
  public void shouldHandleCommandsSpreadOverMultipleLines() {
    final String runScriptContent = "CREATE STREAM S1 \n"
        + "(COL1 BIGINT, COL2 VARCHAR)\n"
        + " WITH \n"
        + "(KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');\n";

    final List<?> parsedStatements = ksqlEngine.parse(runScriptContent);

    assertThat(parsedStatements, hasSize(1));
  }

  @Test
  public void shouldThrowIfSchemaNotPresent() {
    // Given:
    givenTopicsExist("bar");

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "The statement does not define any columns.")));
    expectedException.expect(statementText(is(
        "create stream bar with (value_format='avro', kafka_topic='bar');")));

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine,
        "create stream bar with (value_format='avro', kafka_topic='bar');",
        KSQL_CONFIG,
        Collections.emptyMap());
  }

  @Test
  public void shouldFailIfAvroSchemaNotEvolvable() {
    // Given:
    givenTopicWithSchema("T", Schema.create(Type.INT));

    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Cannot register avro schema for T as the schema registry rejected it, "
            + "(maybe schema evolution issues?)")));
    expectedException.expect(statementText(is(
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;")));

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine,
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;",
        KSQL_CONFIG,
        Collections.emptyMap());
  }

  @Test
  public void shouldNotFailIfAvroSchemaEvolvable() {
    // Given:
    final Schema evolvableSchema = SchemaBuilder
        .record("Test").fields()
        .nullableInt("f1", 1)
        .endRecord();

    givenTopicWithSchema("T", evolvableSchema);

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine,
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;",
        KSQL_CONFIG,
        Collections.emptyMap());

    // Then:
    assertThat(metaStore.getSource("T"), is(notNullValue()));
  }

  @Test
  public void shouldNotDeleteSchemaNorTopicForTable() throws Exception {
    // Given:
    givenTopicsExist("BAR");
    final QueryMetadata query = KsqlEngineTestUtil.execute(ksqlEngine,
        "create table bar with (value_format = 'avro') as select * from test2;",
        KSQL_CONFIG, Collections.emptyMap()).get(0);

    query.close();

    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();

    schemaRegistryClient.register("BAR-value", schema);

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine, "DROP TABLE bar;", KSQL_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(serviceContext.getTopicClient().isTopicExists("BAR"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
  }

  @Test
  public void shouldCleanUpInternalTopicsOnClose() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.execute(ksqlEngine,
        "select * from test1;",
        KSQL_CONFIG, Collections.emptyMap()).get(0);

    query.start();

    // When:
    query.close();

    // Then:
    verify(topicClient).deleteInternalTopics(query.getQueryApplicationId());
  }

  @Test
  public void shouldNotCleanUpInternalTopicsOnCloseIfQueryNeverStarted() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.execute(ksqlEngine,
        "create stream s1 with (value_format = 'avro') as select * from test1;",
        KSQL_CONFIG, Collections.emptyMap()).get(0);

    // When:
    query.close();

    // Then:
    verify(topicClient, never()).deleteInternalTopics(any());
  }

  @Test
  public void shouldRemovePersistentQueryFromEngineWhenClosed() {
    // Given:
    final int startingLiveQueries = ksqlEngine.numberOfLiveQueries();
    final int startingPersistentQueries = ksqlEngine.getPersistentQueries().size();

    final QueryMetadata query = KsqlEngineTestUtil.execute(ksqlEngine,
        "create stream s1 with (value_format = 'avro') as select * from test1;",
        KSQL_CONFIG, Collections.emptyMap()).get(0);

    // When:
    query.close();

    // Then:
    assertThat(ksqlEngine.getPersistentQuery(getQueryId(query)), is(Optional.empty()));
    assertThat(ksqlEngine.numberOfLiveQueries(), is(startingLiveQueries));
    assertThat(ksqlEngine.getPersistentQueries().size(), is(startingPersistentQueries));
  }

  @Test
  public void shouldRemoveTransientQueryFromEngineWhenClosed() {
    // Given:
    final int startingLiveQueries = ksqlEngine.numberOfLiveQueries();

    final QueryMetadata query = KsqlEngineTestUtil.execute(ksqlEngine,
        "select * from test1;",
        KSQL_CONFIG, Collections.emptyMap()).get(0);

    // When:
    query.close();

    // Then:
    assertThat(ksqlEngine.numberOfLiveQueries(), is(startingLiveQueries));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldHandleMultipleStatements() {
    // Given:
    final String sql = ""
        + "-- single line comment\n"
        + "/*\n"
        + "   Multi-line comment\n"
        + "*/\n"
        + "CREATE STREAM S0 (a INT, b VARCHAR) "
        + "      WITH (kafka_topic='s0_topic', value_format='DELIMITED');\n"
        + "\n"
        + "CREATE TABLE T1 (f0 BIGINT, f1 DOUBLE) "
        + "     WITH (kafka_topic='t1_topic', value_format='JSON', key = 'f0');\n"
        + "\n"
        + "CREATE STREAM S1 AS SELECT * FROM S0;\n"
        + "\n"
        + "CREATE STREAM S2 AS SELECT * FROM S0;\n"
        + "\n"
        + "DROP TABLE T1;";

    givenTopicsExist("s0_topic", "t1_topic");

    final List<QueryMetadata> queries = new ArrayList<>();

    // When:
    final List<PreparedStatement<?>> preparedStatements = ksqlEngine.parse(sql).stream()
        .map(stmt ->
        {
          final PreparedStatement<?> prepared = ksqlEngine.prepare(stmt);
          final ExecuteResult result = ksqlEngine.execute(
              ConfiguredStatement.of(prepared, new HashMap<>(), KSQL_CONFIG));
          result.getQuery().ifPresent(queries::add);
          return prepared;
        })
        .collect(Collectors.toList());

    // Then:
    final List<?> statements = preparedStatements.stream()
        .map(PreparedStatement::getStatement)
        .collect(Collectors.toList());

    assertThat(statements, contains(
        instanceOf(CreateStream.class),
        instanceOf(CreateTable.class),
        instanceOf(CreateStreamAsSelect.class),
        instanceOf(CreateStreamAsSelect.class),
        instanceOf(DropTable.class)
    ));

    assertThat(queries, hasSize(2));
  }

  @Test
  public void shouldNotThrowWhenPreparingDuplicateTable() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE TABLE FOO AS SELECT * FROM TEST2; "
            + "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;");

    givenStatementAlreadyExecuted(parsed.get(0));

    // When:
    ksqlEngine.prepare(parsed.get(1));

    // Then: no exception thrown
  }

  @Test
  public void shouldThrowWhenExecutingDuplicateTable() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE TABLE FOO AS SELECT * FROM TEST2; "
            + "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = prepare(parsed.get(1));

    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Cannot add table 'FOO': A table with the same name already exists")));
    expectedException.expect(statementText(is(
        "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;")));

    // When:
    ksqlEngine.execute(ConfiguredStatement.of(prepared, new HashMap<>(), KSQL_CONFIG));
  }

  @Test
  public void shouldThrowWhenPreparingUnknownSource() {
    // Given:
    final ParsedStatement stmt = ksqlEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM UNKNOWN;").get(0);

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Failed to prepare statement: UNKNOWN does not exist.")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO AS SELECT * FROM UNKNOWN;")));

    // When:
    ksqlEngine.prepare(stmt);
  }

  @Test
  public void shouldNotThrowWhenPreparingDuplicateStream() {
    // Given:
    final ParsedStatement stmt = ksqlEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM ORDERS; "
            + "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;").get(0);

    // When:
    ksqlEngine.prepare(stmt);

    // Then: No exception thrown.
  }

  @Test
  public void shouldThrowWhenExecutingDuplicateStream() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM ORDERS; "
            + "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = ksqlEngine.prepare(parsed.get(1));

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is(
        "Cannot add stream 'FOO': A stream with the same name already exists")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;")));

    // When:
    ksqlEngine.execute(ConfiguredStatement.of(prepared, new HashMap<>(), KSQL_CONFIG));
  }

  @Test
  public void shouldThrowWhenExecutingQueriesIfCsasCreatesTable() {
    // Given:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;")));

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine,
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;",
        KSQL_CONFIG, Collections.emptyMap());
  }

  @Test
  public void shouldThrowWhenExecutingQueriesIfCtasCreatesStream() {
    // Given:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.")));
    expectedException.expect(statementText(is(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;")));

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine,
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;",
        KSQL_CONFIG, Collections.emptyMap());
  }

  @Test
  public void shouldThrowWhenTryExecuteCsasThatCreatesTable() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;").get(0));

    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.")));
    expectedException.expect(statementText(is(
        "CREATE STREAM FOO AS SELECT COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;")));

    // When:
    sandbox.execute(ConfiguredStatement.of(statement, new HashMap<>(), KSQL_CONFIG));
  }

  @Test
  public void shouldThrowWhenTryExecuteCtasThatCreatesStream() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;").get(0));

    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(statementText(is("CREATE TABLE FOO AS SELECT * FROM ORDERS;")));
    expectedException.expect(rawMessage(is(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.")));

    // When:
    sandbox.execute(ConfiguredStatement.of(statement, new HashMap<>(), KSQL_CONFIG));
  }

  @Test
  public void shouldThrowIfStatementMissingTopicConfig() {
    final List<ParsedStatement> parsed = parse(
        "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='AVRO');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='AVRO');"
            + "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='JSON');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='JSON');"
    );

    for (final ParsedStatement statement : parsed) {

      try {
        ksqlEngine.prepare(statement);
        Assert.fail();
      } catch (final KsqlStatementException e) {
        assertThat(e.getMessage(), containsString(
            "Missing required property \"KAFKA_TOPIC\" which has no default value."));
      }
    }
  }

  @Test
  public void shouldThrowIfStatementMissingValueFormatConfig() {
    // Given:
    givenTopicsExist("foo");

    final List<ParsedStatement> parsed = parse(
        "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (KAFKA_TOPIC='foo');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (KAFKA_TOPIC='foo');"
    );

    for (final ParsedStatement statement : parsed) {

      try {
        // When:
        ksqlEngine.prepare(statement);

        // Then:
        Assert.fail();
      } catch (final KsqlStatementException e) {
        assertThat(e.getMessage(), containsString(
            "Missing required property \"VALUE_FORMAT\" which has no default value."));
      }
    }
  }

  @Test
  public void shouldThrowOnNoneExecutableDdlStatement() {
    // Given:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expect(rawMessage(is("Statement not executable")));
    expectedException.expect(statementText(is("SHOW STREAMS;")));

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine, "SHOW STREAMS;", KSQL_CONFIG, Collections.emptyMap());
  }

  @Test
  public void shouldNotUpdateMetaStoreDuringTryExecute() {
    // Given:
    final int numberOfLiveQueries = ksqlEngine.numberOfLiveQueries();
    final int numPersistentQueries = ksqlEngine.getPersistentQueries().size();

    final List<ParsedStatement> statements = parse(
            "CREATE STREAM S1 (COL1 BIGINT) WITH (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');"
            + "CREATE TABLE BAR AS SELECT * FROM TEST2;"
            + "CREATE TABLE FOO AS SELECT * FROM TEST2;"
            + "DROP TABLE TEST3;");

    topicClient.preconditionTopicExists("s1_topic", 1, (short) 1, Collections.emptyMap());

    // When:
    statements
        .forEach(stmt -> sandbox.execute(
            ConfiguredStatement.of(sandbox.prepare(stmt), new HashMap<>(), KSQL_CONFIG)));

    // Then:
    assertThat(metaStore.getSource("TEST3"), is(notNullValue()));
    assertThat(metaStore.getQueriesWithSource("TEST2"), is(empty()));
    assertThat(metaStore.getSource("BAR"), is(nullValue()));
    assertThat(metaStore.getSource("FOO"), is(nullValue()));
    assertThat("live", ksqlEngine.numberOfLiveQueries(), is(numberOfLiveQueries));
    assertThat("peristent", ksqlEngine.getPersistentQueries().size(), is(numPersistentQueries));
  }

  @Test
  public void shouldNotCreateAnyTopicsDuringTryExecute() {
    // Given:
    topicClient.preconditionTopicExists("s1_topic", 1, (short) 1, Collections.emptyMap());

    final List<ParsedStatement> statements = parse(
        "CREATE STREAM S1 (COL1 BIGINT) WITH (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON');"
            + "CREATE TABLE BAR AS SELECT * FROM TEST2;"
            + "CREATE TABLE FOO AS SELECT * FROM TEST2;"
            + "DROP TABLE TEST3;");

    // When:
    statements.forEach(
        stmt -> sandbox.execute(ConfiguredStatement.of(sandbox.prepare(stmt), new HashMap<>(), KSQL_CONFIG)));

    // Then:
    assertThat("no topics should be created during a tryExecute call",
        topicClient.createdTopics().keySet(), is(empty()));
  }

  @Test
  public void shouldNotIncrementQueryIdCounterDuringTryExecute() {
    // Given:
    final String sql = "create table foo as select * from test2;";
    final PreparedStatement<?> statement = prepare(parse(sql).get(0));

    // When:
    sandbox.execute(ConfiguredStatement.of(statement, new HashMap<>(), KSQL_CONFIG));

    // Then:
    final List<QueryMetadata> queries = KsqlEngineTestUtil
        .execute(ksqlEngine, sql, KSQL_CONFIG, Collections.emptyMap());
    assertThat("query id of actual execute should not be affected by previous tryExecute",
        ((PersistentQueryMetadata) queries.get(0)).getQueryId(), is(new QueryId("CTAS_FOO_0")));
  }

  @Test
  public void shouldNotRegisterAnySchemasDuringSandboxExecute() throws Exception {
    // Given:
    final List<ParsedStatement> statements = parse(
        "create table foo WITH(VALUE_FORMAT='AVRO') as select * from test2;"
            + "create stream foo2 WITH(VALUE_FORMAT='AVRO') as select * from orders;");

    givenStatementAlreadyExecuted(statements.get(0));

    final PreparedStatement<?> prepared = prepare(statements.get(1));

    // When:
    sandbox.execute(ConfiguredStatement.of(prepared, new HashMap<>(), KSQL_CONFIG));

    // Then:
    verify(schemaRegistryClient, never()).register(any(), any());
  }

  @Test
  public void shouldOnlyUpdateSandboxOnQueryClose() {
    // Given:
    givenSqlAlreadyExecuted("create table bar as select * from test2;");

    final QueryId queryId = ksqlEngine.getPersistentQueries()
        .get(0).getQueryId();

    final PersistentQueryMetadata sandBoxQuery = sandbox.getPersistentQuery(queryId)
        .get();

    // When:
    sandBoxQuery.close();

    // Then:
    assertThat("main engine should not be updated",
        ksqlEngine.getPersistentQuery(queryId), is(not(Optional.empty())));

    assertThat("sand box should be updated",
        sandbox.getPersistentQuery(queryId), is(Optional.empty()));
  }

  @Test
  public void shouldRegisterPersistentQueriesOnlyInSandbox() {
    // Given:
    final PreparedStatement<?> prepared = prepare(parse(
        "create table bar as select * from test2;").get(0));

    // When:
    final ExecuteResult result = sandbox.execute(ConfiguredStatement.of(prepared, new HashMap<>(), KSQL_CONFIG));

    // Then:
    assertThat(result.getQuery(), is(not(Optional.empty())));
    assertThat(sandbox.getPersistentQuery(getQueryId(result.getQuery().get())),
        is(not(Optional.empty())));
    assertThat(ksqlEngine.getPersistentQuery(getQueryId(result.getQuery().get())),
        is(Optional.empty()));
  }

  @Test
  public void shouldExecuteDdlStatement() {
    // Given:
    givenTopicsExist("foo");
    final PreparedStatement<?> statement =
        prepare(parse("CREATE STREAM FOO (a int) WITH (kafka_topic='foo', value_format='json');").get(0));

    // When:
    final ExecuteResult result = sandbox.execute(ConfiguredStatement.of(statement, new HashMap<>(), KSQL_CONFIG));

    // Then:
    assertThat(result.getCommandResult(), is(Optional.of("Stream created")));
  }

  @Test
  public void shouldBeAbleToParseInvalidThings() {
    // Given:
    // No Stream called 'I_DO_NOT_EXIST' exists

    // When:
    final List<ParsedStatement> parsed = ksqlEngine
        .parse("CREATE STREAM FOO AS SELECT * FROM I_DO_NOT_EXIST;");

    // Then:
    assertThat(parsed, hasSize(1));
  }

  @Test
  public void shouldThrowOnPrepareIfSourcesDoNotExist() {
    // Given:
    final ParsedStatement parsed = ksqlEngine
        .parse("CREATE STREAM FOO AS SELECT * FROM I_DO_NOT_EXIST;")
        .get(0);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Failed to prepare statement: I_DO_NOT_EXIST does not exist");

    // When:
    ksqlEngine.prepare(parsed);
  }

  @Test
  public void shouldBeAbleToPrepareTerminateAndDrop() {
    // Given:
    givenSqlAlreadyExecuted("CREATE STREAM FOO AS SELECT * FROM TEST1;");

    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "TERMINATE CSAS_FOO_0;"
            + "DROP STREAM FOO;");

    // When:
    parsed.forEach(ksqlEngine::prepare);

    // Then: did not throw.
  }

  @Test
  public void shouldIgnoreLegacyDeleteTopicPartOfDropCommand() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.execute(ksqlEngine,
        "CREATE STREAM FOO AS SELECT * FROM TEST1;",
        KSQL_CONFIG, Collections.emptyMap()).get(0);
    query.close();

    // When:
    KsqlEngineTestUtil.execute(ksqlEngine, "DROP STREAM FOO DELETE TOPIC;", KSQL_CONFIG, Collections.emptyMap());

    // Then:
    verifyNoMoreInteractions(topicClient);
    verifyNoMoreInteractions(schemaRegistryClient);
  }

  private void givenTopicsExist(final String... topics) {
    givenTopicsExist(1, topics);
  }

  private void givenTopicsExist(final int partitionCount, final String... topics) {
    Arrays.stream(topics)
        .forEach(topic -> topicClient.createTopic(topic, partitionCount, (short) 1));
  }

  private List<ParsedStatement> parse(final String sql) {
    return ksqlEngine.parse(sql);
  }

  private PreparedStatement<?> prepare(final ParsedStatement stmt) {
    return ksqlEngine.prepare(stmt);
  }

  private void givenTopicWithSchema(final String topicName, final Schema schema) {
    try {
      givenTopicsExist(1, topicName);
      schemaRegistryClient.register(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
    } catch (final Exception e) {
      fail("invalid test:" + e.getMessage());
    }
  }

  private static QueryId getQueryId(final QueryMetadata query) {
    return ((PersistentQueryMetadata) query).getQueryId();
  }

  private void givenStatementAlreadyExecuted(
      final ParsedStatement statement
  ) {
    ksqlEngine.execute(
        ConfiguredStatement.of(ksqlEngine.prepare(statement), new HashMap<>(), KSQL_CONFIG));
    sandbox = ksqlEngine.createSandbox(serviceContext);
  }

  private void givenSqlAlreadyExecuted(final String sql) {
    parse(sql).forEach(stmt ->
        ksqlEngine.execute(
            ConfiguredStatement.of(ksqlEngine.prepare(stmt), new HashMap<>(), KSQL_CONFIG)));

    sandbox = ksqlEngine.createSandbox(serviceContext);
  }
}