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

import static io.confluent.ksql.engine.KsqlEngineTestUtil.execute;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.FieldMatchers.hasFullName;
import static io.confluent.ksql.util.KsqlExceptionMatcher.rawMessage;
import static io.confluent.ksql.util.KsqlExceptionMatcher.statementText;
import static java.util.Collections.emptyMap;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.QueryCleanupService.QueryCleanupTask;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.services.FakeKafkaConsumerGroupClient;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "SameParameterValue"})
@RunWith(MockitoJUnitRunner.class)
public class KsqlEngineTest {

  private static final KsqlConfig KSQL_CONFIG = KsqlConfigTestUtil.create("what-eva");

  private MutableMetaStore metaStore;
  @Spy
  private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private final Supplier<SchemaRegistryClient> schemaRegistryClientFactory =
      () -> schemaRegistryClient;

  private KsqlEngine ksqlEngine;
  private ServiceContext serviceContext;
  private ServiceContext sandboxServiceContext;
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
    sandboxServiceContext = sandbox.getServiceContext();
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
        = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(queries, hasSize(2));
    assertThat(queries.get(0), is(instanceOf(PersistentQueryMetadata.class)));
    assertThat(queries.get(1), is(instanceOf(PersistentQueryMetadata.class)));
    assertThat(((PersistentQueryMetadata) queries.get(0)).getSinkName(), is(SourceName.of("BAR")));
    assertThat(((PersistentQueryMetadata) queries.get(1)).getSinkName(), is(SourceName.of("FOO")));
  }

  @Test
  public void shouldNotHaveRowTimeAndRowKeyColumnsInPersistentQueryValueSchema() {
    // When:
    final PersistentQueryMetadata query = (PersistentQueryMetadata) KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table bar as select * from test2;",
        KSQL_CONFIG,
        Collections.emptyMap()
    ).get(0);

    // Then:
    assertThat(query.getLogicalSchema().value(),
        not(hasItem(hasFullName(SystemColumns.ROWTIME_NAME))));

    assertThat(query.getLogicalSchema().value(),
        not(hasItem(hasFullName(SystemColumns.ROWKEY_NAME))));
  }

  @Test
  public void shouldThrowOnTerminateAsNotExecutable() {
    // Given:
    final PersistentQueryMetadata query = (PersistentQueryMetadata) KsqlEngineTestUtil
        .execute(
            serviceContext,
            ksqlEngine,
            "create table bar as select * from test2;",
            KSQL_CONFIG,
            Collections.emptyMap())
        .get(0);

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "TERMINATE " + query.getQueryId() + ";",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString(
        "Statement not executable")));
    assertThat(e, statementText(is(
        "TERMINATE CTAS_BAR_0;")));
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
        .execute(sandboxServiceContext, ConfiguredStatement.of(
            sandbox.prepare(statements.get(1)),
            SessionConfig.of(KSQL_CONFIG, Collections.emptyMap())
        ));

    // Then:
    assertThat(result.getQuery(), is(not(Optional.empty())));
  }

  @Test
  public void shouldThrowWhenExecutingInsertIntoTable() {
    KsqlEngineTestUtil.execute(
        serviceContext, ksqlEngine, "create table bar as select * from test2;", KSQL_CONFIG,
        Collections.emptyMap());

    final ParsedStatement parsed = ksqlEngine.parse("insert into bar select * from test2;").get(0);

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> prepare(parsed)
    );

    // Then:
    assertThat(e, rawMessage(containsString(
        "INSERT INTO can only be used to insert into a stream. BAR is a table.")));
    assertThat(e, statementText(is("insert into bar select * from test2;")));
  }

  @Test
  public void shouldThrowOnInsertIntoStreamWithTableResult() {
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select ordertime, itemid, orderid from orders;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "insert into bar select itemid, count(*) from orders group by itemid;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString(
        "Incompatible data sink and query result. "
            + "Data sink (BAR) type is KSTREAM but select query result is KTABLE.")));
    assertThat(e, statementText(is(
        "insert into bar select itemid, count(*) from orders group by itemid;")));
  }

  @Test
  public void shouldThrowOnInsertIntoWithKeyMismatch() {
    execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from orders;",
        KSQL_CONFIG,
        emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "insert into bar select * from orders partition by orderid;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString("Incompatible schema between results and sink.")));
    assertThat(e, rawMessage(containsString("Result schema is `ORDERID` BIGINT KEY, ")));
    assertThat(e, rawMessage(containsString("Sink schema is `ORDERTIME` BIGINT KEY, ")));

    assertThat(e, statementText(is(
        "insert into bar select * from orders partition by orderid;")));
  }

  @Test
  public void shouldThrowWhenInsertIntoSchemaDoesNotMatch() {
    // Given:
    execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from orders;",
        KSQL_CONFIG,
        emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> execute(
            serviceContext,
            ksqlEngine,
            "insert into bar select orderTime, itemid from orders;",
            KSQL_CONFIG,
            emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(
        containsString(
            "Incompatible schema between results and sink.")));
    assertThat(e, statementText(
        is("insert into bar select orderTime, itemid from orders;")));
  }

  @Test
  public void shouldExecuteInsertIntoWithCustomQueryId() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from orders;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "insert into bar with (query_id='my_insert_id') select * from orders;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(queries, hasSize(1));
    assertThat(queries.get(0).getQueryId(), is(new QueryId("MY_INSERT_ID")));
  }

  @Test
  public void shouldThrowInsertIntoIfCustomQueryIdAlreadyExists() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from orders;"
            + "insert into bar with (query_id='my_insert_id') select * from orders;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final Exception e = assertThrows(Exception.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "insert into bar with (query_id='my_insert_id') select * from orders;",
            KSQL_CONFIG,
            Collections.emptyMap())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Query ID 'MY_INSERT_ID' already exists."));
  }

  @Test
  public void shouldExecuteInsertIntoStream() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from orders;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "insert into bar select * from orders;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(queries, hasSize(1));
  }

  @Test
  public void shouldMaintainOrderOfReturnedQueries() {
    // When:
    final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream foo as select * from orders;"
            + "create stream bar as select * from orders;",
        KSQL_CONFIG, Collections.emptyMap());

    // Then:
    assertThat(queries, hasSize(2));
    assertThat(queries.get(0).getStatementString(), containsString("CREATE STREAM FOO"));
    assertThat(queries.get(1).getStatementString(), containsString("CREATE STREAM BAR"));
  }

  @Test(expected = KsqlStatementException.class)
  public void shouldFailToCreateQueryIfSelectingFromNonExistentEntity() {
    KsqlEngineTestUtil
        .execute(
            serviceContext,
            ksqlEngine,
            "select * from bar;",
            KSQL_CONFIG,
            Collections.emptyMap()
        );
  }

  @Test(expected = ParseFailedException.class)
  public void shouldFailWhenSyntaxIsInvalid() {
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "blah;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );
  }

  @Test
  public void shouldFailDropTableWhenAnotherTableIsReadingTheTable() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from bar;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "drop table bar;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Cannot drop BAR.\n"
            + "The following streams and/or tables read from this source: [FOO].\n"
            + "You need to drop them before dropping BAR.")));
    assertThat(e, statementText(is("drop table bar;")));
  }

  @Test
  public void shouldFailDropStreamWhenAnotherStreamIsReadingTheTable() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from test1;"
            + "create stream foo as select * from bar;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "drop stream bar;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Cannot drop BAR.\n"
            + "The following streams and/or tables read from this source: [FOO].\n"
            + "You need to drop them before dropping BAR.")));
    assertThat(e, statementText(is("drop stream bar;")));
  }

  @Test
  public void shouldFailDropStreamWhenAnInsertQueryIsWritingTheStream() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from test1;"
            + "insert into bar select * from test1;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "drop stream bar;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Cannot drop BAR.\n"
            + "The following queries read from this source: [].\n"
            + "The following queries write into this source: [INSERTQUERY_1].\n"
            + "You need to terminate them before dropping BAR.")));
    assertThat(e, statementText(is("drop stream bar;")));
  }

  @Test
  public void shouldFailDropStreamWhenAnInsertQueryIsReadingTheStream() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream bar as select * from test1;"
            + "create stream foo as select * from test1;"
            + "insert into foo select * from bar;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "drop stream bar;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Cannot drop BAR.\n"
            + "The following queries read from this source: [INSERTQUERY_2].\n"
            + "The following queries write into this source: [].\n"
            + "You need to terminate them before dropping BAR.")));
    assertThat(e, statementText(is("drop stream bar;")));
  }

  @Test
  public void shouldDropTableAndTerminateQuery() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table foo as select * from test2;"
            + "create table bar as select * from foo;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "drop table bar;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("bar")), nullValue());

    // Only CTAS_FOO_0 query must be running
    assertThat(ksqlEngine.getPersistentQueries().size(), is(1));
    assertThat(ksqlEngine.getPersistentQuery(new QueryId("CTAS_FOO_0")).get(), not(nullValue()));
  }

  @Test
  public void shouldDropStreamAndTerminateQuery() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream foo as select * from test1;"
            + "create stream bar as select * from foo;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "drop stream bar;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("bar")), nullValue());

    // Only CSAS_FOO_0 query must be running
    assertThat(ksqlEngine.getPersistentQueries().size(), is(1));
    assertThat(ksqlEngine.getPersistentQuery(new QueryId("CSAS_FOO_0")).get(), not(nullValue()));
  }

  @Test
  public void shouldDropStreamIfQueryWasTerminatedManually() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream foo as select * from test1;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    ksqlEngine.getPersistentQuery(new QueryId("CSAS_FOO_0")).get().close();
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
                "drop stream foo;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("foo")), nullValue());

    // Only CSAS_FOO_0 query must be running
    assertThat(ksqlEngine.getPersistentQueries().size(), is(0));
  }

  @Test
  public void shouldDropTableIfQueryWasTerminatedManually() {
    // Given:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table foo as select * from test2;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // When:
    ksqlEngine.getPersistentQuery(new QueryId("CTAS_FOO_0")).get().close();
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "drop table foo;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("foo")), nullValue());

    // Only CSAS_FOO_0 query must be running
    assertThat(ksqlEngine.getPersistentQueries().size(), is(0));
  }

  @Test
  public void shouldFailDDLStatementIfTopicDoesNotExist() {
    // Given:
    final ParsedStatement stmt = parse(
        "CREATE STREAM S1_NOTEXIST (COL1 BIGINT, COL2 VARCHAR) "
            + "WITH  (KAFKA_TOPIC = 'S1_NOTEXIST', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');").get(0);

    final PreparedStatement<?> prepared = prepare(stmt);

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement.of(prepared, SessionConfig.of(KSQL_CONFIG, Collections.emptyMap()))
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("Kafka topic does not exist: S1_NOTEXIST"));
  }

  @Test
  public void shouldDropTableIfAllReferencedQueriesTerminated() {
    // Given:
    final QueryMetadata secondQuery = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table bar as select * from test2;"
            + "create table foo as select * from test2;",
        KSQL_CONFIG,
        Collections.emptyMap())
        .get(1);

    secondQuery.close();

    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "drop table foo;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("foo")), nullValue());
  }

  @Test
  public void shouldNotEnforceTopicExistenceWhileParsing() {
    final String runScriptContent = "CREATE STREAM S1 (COL1 BIGINT, COL2 VARCHAR) "
        + "WITH  (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');\n"
        + "CREATE TABLE T1 AS SELECT COL1, count(*) FROM "
        + "S1 GROUP BY COL1;\n"
        + "CREATE STREAM S2 (C1 BIGINT, C2 BIGINT) "
        + "WITH (KAFKA_TOPIC = 'T1', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');\n";

    final List<?> parsedStatements = ksqlEngine.parse(runScriptContent);

    assertThat(parsedStatements.size(), equalTo(3));
  }

  @Test
  public void shouldThrowFromSandBoxOnPrepareIfSourceTopicDoesNotExist() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM S1 (COL1 BIGINT) "
            + "WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');").get(0));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement.of(statement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of()))
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Kafka topic does not exist: i_do_not_exist")));
    assertThat(e, statementText(is(
        "CREATE STREAM S1 (COL1 BIGINT)"
            + " WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');")));
  }

  @Test
  public void shouldThrowFromExecuteIfSourceTopicDoesNotExist() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM S1 (COL1 BIGINT) "
            + "WITH (KAFKA_TOPIC = 'i_do_not_exist', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');").get(0));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> ksqlEngine.execute(
            serviceContext,
            ConfiguredStatement.of(statement, SessionConfig.of(KSQL_CONFIG, ImmutableMap.of()))
        )
    );

    // Then:
    assertThat(e, rawMessage(is("Kafka topic does not exist: i_do_not_exist")));
  }

  @Test
  public void shouldHandleCommandsSpreadOverMultipleLines() {
    final String runScriptContent = "CREATE STREAM S1 \n"
        + "(COL1 BIGINT, COL2 VARCHAR)\n"
        + " WITH \n"
        + "(KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON', KEY_FORMAT = 'KAFKA');\n";

    final List<?> parsedStatements = ksqlEngine.parse(runScriptContent);

    assertThat(parsedStatements, hasSize(1));
  }

  @Test
  public void shouldThrowIfSchemaNotPresent() {
    // Given:
    givenTopicsExist("bar");

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> execute(
            serviceContext,
            ksqlEngine,
            "create stream bar with (key_format='kafka', value_format='avro', kafka_topic='bar');",
            KSQL_CONFIG,
            emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(
        containsString(
            "The statement does not define any columns.")));
    assertThat(e, statementText(
        is(
            "create stream bar with (key_format='kafka', value_format='avro', kafka_topic='bar');")));
  }

  @Test
  public void shouldNotFailIfAvroSchemaEvolvable() {
    // Given:
    final Schema evolvableSchema = SchemaBuilder
        .record("Test").fields()
        .nullableInt("f1", 1)
        .endRecord();

    givenTopicWithValueSchema("T", evolvableSchema);

    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "CREATE TABLE T WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM TEST2;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("T")), is(notNullValue()));
  }

  @Test
  public void shouldNotDeleteSchemaNorTopicForTable() throws Exception {
    // Given:
    givenTopicsExist("BAR");
    final QueryMetadata query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table bar with (value_format = 'avro') as select * from test2;",
        KSQL_CONFIG, Collections.emptyMap()
    ).get(0);

    query.close();

    final Schema schema = SchemaBuilder
        .record("Test").fields()
        .name("clientHash").type().fixed("MD5").size(16).noDefault()
        .endRecord();

    schemaRegistryClient.register("BAR-value", new AvroSchema(schema));

    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "DROP TABLE bar;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

    // Then:
    assertThat(serviceContext.getTopicClient().isTopicExists("BAR"), equalTo(true));
    assertThat(schemaRegistryClient.getAllSubjects(), hasItem("BAR-value"));
  }

  @Test
  public void shouldCleanUpInternalTopicsOnClose() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.executeQuery(
        serviceContext,
        ksqlEngine,
        "select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    query.start();

    // When:
    query.close();

    // Then:
    awaitCleanupComplete();
    verify(topicClient).deleteInternalTopics(query.getQueryApplicationId());
  }

  @Test
  public void shouldCleanUpInternalTopicsOnEngineCloseForTransientQueries() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.executeQuery(
        serviceContext,
        ksqlEngine,
        "select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    query.start();

    // When:
    ksqlEngine.close();

    // Then:
    verify(topicClient).deleteInternalTopics(query.getQueryApplicationId());
  }

  @Test
  public void shouldHardDeleteSchemaOnEngineCloseForTransientQueries() throws IOException, RestClientException {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.executeQuery(
        serviceContext,
        ksqlEngine,
        "select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );
    final String internalTopic1Val = KsqlConstants.getSRSubject(
        query.getQueryApplicationId() + "-subject1" + KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX, false);
    final String internalTopic2Val = KsqlConstants.getSRSubject(
        query.getQueryApplicationId() + "-subject3" + KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX, false);
    final String internalTopic1Key = KsqlConstants.getSRSubject(
        query.getQueryApplicationId() + "-subject1" + KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX, true);
    final String internalTopic2Key = KsqlConstants.getSRSubject(
        query.getQueryApplicationId() + "-subject3" + KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX, true);

    when(schemaRegistryClient.getAllSubjects()).thenReturn(
        Arrays.asList(
            internalTopic1Val,
            internalTopic1Key,
            "subject2",
            internalTopic2Val,
            internalTopic2Key));

    query.start();

    // When:
    query.close();

    // Then:
    awaitCleanupComplete();
    verify(schemaRegistryClient, times(4)).deleteSubject(any());
    verify(schemaRegistryClient).deleteSubject(internalTopic1Val, true);
    verify(schemaRegistryClient).deleteSubject(internalTopic2Val, true);
    verify(schemaRegistryClient).deleteSubject(internalTopic1Key, true);
    verify(schemaRegistryClient).deleteSubject(internalTopic2Key, true);
    verify(schemaRegistryClient, never()).deleteSubject("subject2");
  }

  @Test
  public void shouldCleanUpConsumerGroupsOnClose() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create table bar as select * from test2;",
        KSQL_CONFIG, Collections.emptyMap()
    ).get(0);

    query.start();

    // When:
    query.close();

    // Then:
    awaitCleanupComplete();
    final Set<String> deletedConsumerGroups = (
        (FakeKafkaConsumerGroupClient) serviceContext.getConsumerGroupClient()
    ).getDeletedConsumerGroups();

    assertThat(
        Iterables.getOnlyElement(deletedConsumerGroups),
        containsString("_confluent-ksql-default_query_CTAS_BAR_0"));
  }

  @Test
  public void shouldCleanUpTransientConsumerGroupsOnClose() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.executeQuery(
        serviceContext,
        ksqlEngine,
        "select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    query.start();

    // When:
    query.close();

    // Then:
    awaitCleanupComplete();
    final Set<String> deletedConsumerGroups = (
        (FakeKafkaConsumerGroupClient) serviceContext.getConsumerGroupClient()
    ).getDeletedConsumerGroups();

    assertThat(
        Iterables.getOnlyElement(deletedConsumerGroups),
        containsString("_confluent-ksql-default_transient_"));
  }

  @Test
  public void shouldNotCleanUpInternalTopicsOnEngineCloseForPersistentQueries() {
    // Given:
    final List<QueryMetadata> query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream persistent as select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    query.get(0).start();

    // When:
    ksqlEngine.close();

    // Then (there are no transient queries, so no internal topics should be deleted):
    verify(topicClient, never()).deleteInternalTopics(any());
  }

  @Test
  public void shouldCleanUpInternalTopicsOnQueryCloseForPersistentQueries() {
    // Given:
    final List<QueryMetadata> query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream persistent as select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    query.get(0).start();

    // When:
    query.get(0).close();

    // Then (there are no transient queries, so no internal topics should be deleted):
    awaitCleanupComplete();
    verify(topicClient).deleteInternalTopics(query.get(0).getQueryApplicationId());
  }

  @Test
  public void shouldNotHardDeleteSubjectForPersistentQuery() throws IOException, RestClientException {
    // Given:
    final List<QueryMetadata> query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream persistent as select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );
    final String applicationId = query.get(0).getQueryApplicationId();
    final String internalTopic1Val = KsqlConstants.getSRSubject(
        applicationId + "-subject1" + KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX, false);
    final String internalTopic2Val = KsqlConstants.getSRSubject(
        applicationId + "-subject3" + KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX, false);
    final String internalTopic1Key = KsqlConstants.getSRSubject(
        applicationId + "-subject1" + KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX, true);
    final String internalTopic2Key = KsqlConstants.getSRSubject(
        applicationId + "-subject3" + KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX, true);
    when(schemaRegistryClient.getAllSubjects()).thenReturn(
        Arrays.asList(
            internalTopic1Val,
            internalTopic1Key,
            "subject2",
            internalTopic2Val,
            internalTopic2Key));
    query.get(0).start();

    // When:
    query.get(0).close();

    // Then:
    awaitCleanupComplete();
    verify(schemaRegistryClient, times(4)).deleteSubject(any());
    verify(schemaRegistryClient, never()).deleteSubject(internalTopic1Val, true);
    verify(schemaRegistryClient, never()).deleteSubject(internalTopic1Key, true);
    verify(schemaRegistryClient, never()).deleteSubject(internalTopic2Val, true);
    verify(schemaRegistryClient, never()).deleteSubject(internalTopic2Key, true);
  }

  @Test
  public void shouldNotCleanUpInternalTopicsOnCloseIfQueryNeverStarted() {
    // Given:
    final QueryMetadata query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream s1 with (value_format = 'avro') as select * from test1;",
        KSQL_CONFIG, Collections.emptyMap()
    ).get(0);

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

    final QueryMetadata query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream s1 with (value_format = 'avro') as select * from test1;",
        KSQL_CONFIG,
        Collections.emptyMap()
    ).get(0);

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

    final QueryMetadata query = KsqlEngineTestUtil.executeQuery(
        serviceContext,
        ksqlEngine,
        "select * from test1 EMIT CHANGES;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    // When:
    query.close();

    // Then:
    assertThat(ksqlEngine.numberOfLiveQueries(), is(startingLiveQueries));
  }

  @Test
  public void shouldSetKsqlSinkForSinks() {
    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "create stream s as select * from orders;"
            + "create table t as select itemid, count(*) from orders group by itemid;",
        KSQL_CONFIG, Collections.emptyMap()
    );

    // Then:
    assertThat(metaStore.getSource(SourceName.of("S")).isCasTarget(), is(true));
    assertThat(metaStore.getSource(SourceName.of("T")).isCasTarget(), is(true));
  }

  @Test
  public void shouldThrowIfLeftTableNotJoiningOnTableKey() {

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
        + "      WITH (kafka_topic='s0_topic', value_format='DELIMITED', key_format='KAFKA');\n"
        + "\n"
        + "CREATE TABLE T1 (f0 BIGINT PRIMARY KEY, f1 DOUBLE) "
        + "     WITH (kafka_topic='t1_topic', value_format='JSON', key_format='KAFKA');\n"
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
              serviceContext,
              ConfiguredStatement
                  .of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>())));
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
  public void shouldNotThrowWhenExecutingDuplicateTableWithIfNotExists() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2; "
            + "CREATE TABLE IF NOT EXISTS FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = prepare(parsed.get(1));

    // When:
    ExecuteResult executeResult = ksqlEngine.execute(
        serviceContext, ConfiguredStatement.
            of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
    );

    // Then:
    assertThat(executeResult.getQuery(), is(Optional.empty()));
    assertThat(executeResult.getCommandResult(),
        is(Optional.of("Cannot add table `FOO`: A table with the same name already exists.")));
  }

  @Test
  public void shouldThrowWhenExecutingDuplicateTable() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE TABLE FOO AS SELECT * FROM TEST2; "
            + "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = prepare(parsed.get(1));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> ksqlEngine.execute(
            serviceContext,
            ConfiguredStatement
                .of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Cannot add table 'FOO': A table with the same name already exists")));
    assertThat(e, statementText(is(
        "CREATE TABLE FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM TEST2;")));
  }

  @Test
  public void shouldThrowWhenPreparingUnknownSource() {
    // Given:
    final ParsedStatement stmt = ksqlEngine.parse(
        "CREATE STREAM FOO AS SELECT * FROM UNKNOWN;").get(0);

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> ksqlEngine.prepare(stmt)
    );

    // Then:
    assertThat(e.getMessage(), containsString("UNKNOWN does not exist."));
    assertThat(e, statementText(is(
        "CREATE STREAM FOO AS SELECT * FROM UNKNOWN;")));
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
  public void shouldNotThrowWhenExecutingDuplicateStreamWithIfNotExists() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS; "
            + "CREATE STREAM IF NOT EXISTS FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = prepare(parsed.get(1));

    // When:
    ExecuteResult executeResult = ksqlEngine.execute(
        serviceContext, ConfiguredStatement.
            of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
    );

    // Then:
    assertThat(executeResult.getQuery(), is(Optional.empty()));
    assertThat(executeResult.getCommandResult(),
        is(Optional.of("Cannot add stream `FOO`: A stream with the same name already exists.")));
  }

  @Test
  public void shouldThrowWhenExecutingDuplicateStream() {
    // Given:
    final List<ParsedStatement> parsed = ksqlEngine.parse(
        "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS; "
            + "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;");

    givenStatementAlreadyExecuted(parsed.get(0));

    final PreparedStatement<?> prepared = ksqlEngine.prepare(parsed.get(1));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> ksqlEngine.execute(
            serviceContext,
            ConfiguredStatement
                .of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
        )
    );

    // Then:
    assertThat(e, rawMessage(is(
        "Cannot add stream 'FOO': A stream with the same name already exists")));
    assertThat(e, statementText(is(
        "CREATE STREAM FOO WITH (KAFKA_TOPIC='BAR') AS SELECT * FROM ORDERS;")));
  }

  @Test
  public void shouldThrowWhenExecutingQueriesIfCsasCreatesTable() {
    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "CREATE STREAM FOO AS SELECT ORDERID, COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;",
            KSQL_CONFIG, Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.")));
    assertThat(e, statementText(is(
        "CREATE STREAM FOO AS SELECT ORDERID, COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;")));
  }

  @Test
  public void shouldThrowWhenExecutingQueriesIfCtasCreatesStream() {
    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "CREATE TABLE FOO AS SELECT * FROM ORDERS;",
            KSQL_CONFIG, Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.")));
    assertThat(e, statementText(is(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;")));
  }

  @Test
  public void shouldThrowWhenTryExecuteCsasThatCreatesTable() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE STREAM FOO AS SELECT ORDERID, COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;").get(0));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> sandbox.execute(
            serviceContext,
            ConfiguredStatement
                .of(statement, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead.")));
    assertThat(e, statementText(is(
        "CREATE STREAM FOO AS SELECT ORDERID, COUNT(ORDERID) FROM ORDERS GROUP BY ORDERID;")));
  }

  @Test
  public void shouldThrowWhenTryExecuteCtasThatCreatesStream() {
    // Given:
    final PreparedStatement<?> statement = prepare(parse(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;").get(0));

    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> sandbox.execute(
            serviceContext,
            ConfiguredStatement
                .of(statement, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
        )
    );

    // Then:
    assertThat(e, statementText(is(
        "CREATE TABLE FOO AS SELECT * FROM ORDERS;")));
    assertThat(e, rawMessage(is(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.")));
  }

  @Test
  public void shouldThrowIfStatementMissingTopicConfig() {
    final List<ParsedStatement> parsed = parse(
        "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='AVRO', KEY_FORMAT='KAFKA');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='AVRO', KEY_FORMAT='KAFKA');"
            + "CREATE TABLE FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='JSON', KEY_FORMAT='KAFKA');"
            + "CREATE STREAM FOO (viewtime BIGINT, pageid VARCHAR) WITH (VALUE_FORMAT='JSON', KEY_FORMAT='KAFKA');"
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
  public void shouldThrowOnUnsupportedKeyFormatForCreateSource() {
    // Given:
    givenTopicsExist("foo");
    final PreparedStatement<?> prepared =
        prepare(parse("CREATE STREAM FOO (a int) WITH (kafka_topic='foo', value_format='json', key_format='protobuf');").get(0));

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement.of(prepared, SessionConfig.of(KSQL_CONFIG, Collections.emptyMap()))
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString("The key format 'PROTOBUF' is not currently supported."));
  }

  @Test
  public void shouldThrowOnUnsupportedKeyFormatForCSAS() {
    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "CREATE STREAM FOO WITH (KEY_FORMAT='PROTOBUF') AS SELECT * FROM ORDERS;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(containsString("The key format 'PROTOBUF' is not currently supported.")));
  }

  @Test
  public void shouldThrowOnNoneExecutableDdlStatement() {
    // When:
    final KsqlStatementException e = assertThrows(
        KsqlStatementException.class,
        () -> KsqlEngineTestUtil.execute(
            serviceContext,
            ksqlEngine,
            "SHOW STREAMS;",
            KSQL_CONFIG,
            Collections.emptyMap()
        )
    );

    // Then:
    assertThat(e, rawMessage(is("Statement not executable")));
    assertThat(e, statementText(is("SHOW STREAMS;")));
  }

  @Test
  public void shouldNotUpdateMetaStoreDuringTryExecute() {
    // Given:
    final int numberOfLiveQueries = ksqlEngine.numberOfLiveQueries();
    final int numPersistentQueries = ksqlEngine.getPersistentQueries().size();

    final List<ParsedStatement> statements = parse(
        "CREATE STREAM S1 (COL1 BIGINT) WITH (KAFKA_TOPIC = 's1_topic', VALUE_FORMAT = 'JSON', KEY_FORMAT='KAFKA');"
            + "CREATE TABLE BAR AS SELECT * FROM TEST2;"
            + "CREATE TABLE FOO AS SELECT * FROM TEST2;"
            + "DROP TABLE TEST3;");

    topicClient.preconditionTopicExists("s1_topic", 1, (short) 1, Collections.emptyMap());

    // When:
    statements
        .forEach(stmt -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement
                .of(sandbox.prepare(stmt), SessionConfig.of(KSQL_CONFIG, new HashMap<>())
                )));

    // Then:
    assertThat(metaStore.getSource(SourceName.of("TEST3")), is(notNullValue()));
    assertThat(metaStore.getSource(SourceName.of("BAR")), is(nullValue()));
    assertThat(metaStore.getSource(SourceName.of("FOO")), is(nullValue()));
    assertThat("live", ksqlEngine.numberOfLiveQueries(), is(numberOfLiveQueries));
    assertThat("peristent", ksqlEngine.getPersistentQueries().size(), is(numPersistentQueries));
  }

  @Test
  public void shouldNotCreateAnyTopicsDuringTryExecute() {
    // Given:
    topicClient.preconditionTopicExists("s1_topic", 1, (short) 1, Collections.emptyMap());

    final List<ParsedStatement> statements = parse(
        "CREATE STREAM S1 (COL1 BIGINT) WITH (KAFKA_TOPIC = 's1_topic', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');"
            + "CREATE TABLE BAR AS SELECT * FROM TEST2;"
            + "CREATE TABLE FOO AS SELECT * FROM TEST2;"
            + "DROP TABLE TEST3;");

    // When:
    statements.forEach(
        stmt -> sandbox.execute(
            sandboxServiceContext,
            ConfiguredStatement
                .of(sandbox.prepare(stmt), SessionConfig.of(KSQL_CONFIG, new HashMap<>())
                ))
    );

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
    sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement
            .of(statement, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
    );

    // Then:
    final List<QueryMetadata> queries = KsqlEngineTestUtil
        .execute(serviceContext, ksqlEngine, sql, KSQL_CONFIG, Collections.emptyMap());
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
    sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement
            .of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
    );

    // Then:
    verify(schemaRegistryClient, never()).register(any(), any(AvroSchema.class));
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
    final ExecuteResult result = sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement
            .of(prepared, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
    );

    // Then:
    assertThat(result.getQuery(), is(not(Optional.empty())));
    assertThat(sandbox.getPersistentQuery(getQueryId(result.getQuery().get())),
        is(not(Optional.empty())));
    assertThat(ksqlEngine.getPersistentQuery(getQueryId(result.getQuery().get())),
        is(Optional.empty()));
  }

  @Test
  public void shouldNotStartKafkaStreamsInSandbox() {
    // Given:
    givenSqlAlreadyExecuted("create table bar as select * from test2;");
    final QueryMetadata query = ksqlEngine.getPersistentQueries().get(0);

    // When:
    sandbox.getPersistentQuery(query.getQueryId()).get().start();

    // Then:
    assertThat(ksqlEngine.getPersistentQuery(query.getQueryId()).get().getState(),
        is(KafkaStreams.State.CREATED));
  }

  @Test
  public void shouldNotStopKafkaStreamsInSandbox() {
    // Given:
    givenSqlAlreadyExecuted("create table bar as select * from test2;");
    final QueryMetadata query = ksqlEngine.getPersistentQueries().get(0);
    ksqlEngine.getPersistentQuery(query.getQueryId()).get().start();

    // When:
    sandbox.getPersistentQuery(query.getQueryId()).get().stop();

    // Then:
    assertThat(ksqlEngine.getPersistentQuery(query.getQueryId()).get().getState(),
        is(KafkaStreams.State.REBALANCING));
  }

  @Test
  public void shouldNotCloseKafkaStreamsInSandbox() {
    // Given:
    givenSqlAlreadyExecuted("create table bar as select * from test2;");
    final QueryMetadata query = ksqlEngine.getPersistentQueries().get(0);
    ksqlEngine.getPersistentQuery(query.getQueryId()).get().start();

    // When:
    sandbox.getPersistentQuery(query.getQueryId()).get().close();

    // Then:
    assertThat(ksqlEngine.getPersistentQuery(query.getQueryId()).get().getState(),
        is(KafkaStreams.State.REBALANCING));
  }

  @Test
  public void shouldExecuteDdlStatement() {
    // Given:
    givenTopicsExist("foo");
    final PreparedStatement<?> statement =
        prepare(parse("CREATE STREAM FOO (a int) WITH (kafka_topic='foo', value_format='json', key_format='kafka');").get(0));

    // When:
    final ExecuteResult result = sandbox.execute(
        sandboxServiceContext,
        ConfiguredStatement
            .of(statement, SessionConfig.of(KSQL_CONFIG, new HashMap<>()))
    );

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

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ksqlEngine.prepare(parsed)
    );

    // Then:
    assertThat(e.getMessage(), containsString("I_DO_NOT_EXIST does not exist"));
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
    final QueryMetadata query = KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "CREATE STREAM FOO AS SELECT * FROM TEST1;",
        KSQL_CONFIG, Collections.emptyMap()
    ).get(0);
    query.close();

    // When:
    KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        "DROP STREAM FOO DELETE TOPIC;",
        KSQL_CONFIG,
        Collections.emptyMap()
    );

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

  private void awaitCleanupComplete() {
    // add a task to the end of the queue to make sure that
    // we've finished processing everything up until this point
    ksqlEngine.getCleanupService().addCleanupTask(new QueryCleanupTask(serviceContext, "", false) {
      @Override
      public void run() {
        // do nothing
      }
    });

    // busy wait is fine here because this should only be
    // used in tests - if we ever have the need to make this
    // production ready, then we should properly implement this
    // with a condition variable wait/notify pattern
    while (!ksqlEngine.getCleanupService().isEmpty()) {
      Thread.yield();
    }
  }

  private void givenTopicWithValueSchema(final String topicName, final Schema schema) {
    try {
      givenTopicsExist(1, topicName);
      schemaRegistryClient.register(
          KsqlConstants.getSRSubject(topicName, false), new AvroSchema(schema));
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
        serviceContext,
        ConfiguredStatement
            .of(ksqlEngine.prepare(statement), SessionConfig.of(KSQL_CONFIG, new HashMap<>())
            ));
    sandbox = ksqlEngine.createSandbox(serviceContext);
  }

  private void givenSqlAlreadyExecuted(final String sql) {
    parse(sql).forEach(stmt -> ksqlEngine.execute(
        serviceContext,
        ConfiguredStatement
            .of(ksqlEngine.prepare(stmt), SessionConfig.of(KSQL_CONFIG, new HashMap<>())
            )));

    sandbox = ksqlEngine.createSandbox(serviceContext);
  }
}
