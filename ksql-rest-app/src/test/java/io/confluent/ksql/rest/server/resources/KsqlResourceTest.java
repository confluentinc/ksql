/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import static io.confluent.ksql.rest.server.computation.CommandId.Action.CREATE;
import static io.confluent.ksql.rest.server.computation.CommandId.Type.TOPIC;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.KsqlTopicInfo;
import io.confluent.ksql.rest.entity.KsqlTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.computation.CommandStatusFuture;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import io.confluent.rest.RestConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class KsqlResourceTest {

  private static final long STATE_CLEANUP_DELAY_MS_DEFAULT = 10 * 60 * 1000L;
  private static final int FETCH_MIN_BYTES_DEFAULT = 1;
  private static final long BUFFER_MEMORY_DEFAULT = 32 * 1024 * 1024L;
  private static final Duration DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT = Duration.ofMillis(1000);
  private static final KsqlRequest VALID_EXECUTABLE_REQUEST = new KsqlRequest(
      "CREATE STREAM S AS SELECT * FROM test_stream;",
      ImmutableMap.of(KsqlConfig.KSQL_WINDOWED_SESSION_KEY_LEGACY_CONFIG, true),
      0L);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlConfig ksqlConfig;
  private KsqlRestConfig ksqlRestConfig;
  private KafkaTopicClient kafkaTopicClient;
  private KsqlEngine realEngine;
  private KsqlEngine ksqlEngine;
  @Mock
  private CommandStore commandStore;
  @Mock
  private ActivenessRegistrar activenessRegistrar;
  private KsqlResource ksqlResource;
  private SchemaRegistryClient schemaRegistryClient;
  private QueuedCommandStatus commandStatus;
  private MetaStoreImpl metaStore;
  private ServiceContext serviceContext;

  @Before
  public void setUp() throws IOException, RestClientException {
    commandStatus = new QueuedCommandStatus(
        0, new CommandStatusFuture(new CommandId(TOPIC, "whateva", CREATE)));
    serviceContext = TestServiceContext.create();
    kafkaTopicClient = serviceContext.getTopicClient();
    schemaRegistryClient = serviceContext.getSchemaRegistryClient();
    registerSchema(schemaRegistryClient);
    ksqlRestConfig = new KsqlRestConfig(getDefaultKsqlConfig());
    ksqlConfig = new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties());

    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());

    realEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore
    );

    ksqlEngine = realEngine;

    addTestTopicAndSources();

    setUpKsqlResource();

    when(commandStore.enqueueCommand(any(), any(), any(), any())).thenReturn(commandStatus);
  }

  @After
  public void tearDown() {
    realEngine.close();
    serviceContext.close();
  }

  @Test
  public void shouldInstantRegisterTopic() {
    // When:
    final CommandStatusEntity result = makeSingleRequest(
        "REGISTER TOPIC FOO WITH (kafka_topic='bar', value_format='json');",
        CommandStatusEntity.class);

    // Then:
    assertThat(result, is(new CommandStatusEntity(
        "REGISTER TOPIC FOO WITH (kafka_topic='bar', value_format='json');",
        commandStatus.getCommandId(), commandStatus.getStatus(), 0L)));
  }

  @Test
  public void shouldListRegisteredTopics() {
    // When:
    final KsqlTopicsList ksqlTopicsList = makeSingleRequest(
        "LIST REGISTERED TOPICS;", KsqlTopicsList.class);

    // Then:
    final Collection<KsqlTopicInfo> expectedTopics = ksqlEngine.getMetaStore()
        .getAllKsqlTopics().values().stream()
        .map(KsqlTopicInfo::new)
        .collect(Collectors.toList());

    assertThat(ksqlTopicsList.getTopics(), is(expectedTopics));
  }

  @Test
  public void shouldShowNoQueries() {
    // When:
    final Queries queries = makeSingleRequest("SHOW QUERIES;", Queries.class);

    // Then:
    assertThat(queries.getQueries(), is(empty()));
  }

  @Test
  public void shouldListFunctions() {
    // When:
    final FunctionNameList functionList = makeSingleRequest(
        "LIST FUNCTIONS;", FunctionNameList.class);

    // Then:
    assertThat(functionList.getFunctions(), hasItems(
        new SimpleFunctionInfo("EXTRACTJSONFIELD", FunctionType.scalar),
        new SimpleFunctionInfo("ARRAYCONTAINS", FunctionType.scalar),
        new SimpleFunctionInfo("CONCAT", FunctionType.scalar),
        new SimpleFunctionInfo("TOPK", FunctionType.aggregate),
        new SimpleFunctionInfo("MAX", FunctionType.aggregate)));

    assertThat("shouldn't contain internal functions", functionList.getFunctions(),
        not(hasItem(new SimpleFunctionInfo("FETCH_FIELD_FROM_STRUCT", FunctionType.scalar))));
  }

  @Test
  public void shouldShowStreamsExtended() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("FIELD1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("FIELD2", Schema.OPTIONAL_STRING_SCHEMA);

    ensureSource(
        DataSource.DataSourceType.KSTREAM, "new_stream", "new_topic",
        "new_ksql_topic", schema);

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "SHOW STREAMS EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        new SourceDescription(
            ksqlEngine.getMetaStore().getSource("TEST_STREAM"),
            true, "JSON", Collections.emptyList(), Collections.emptyList(),
            kafkaTopicClient),
        new SourceDescription(
            ksqlEngine.getMetaStore().getSource("new_stream"),
            true, "JSON", Collections.emptyList(), Collections.emptyList(),
            kafkaTopicClient)));
  }

  @Test
  public void shouldShowTablesExtended() {
    // Given:
    final Schema schema = SchemaBuilder.struct()
        .field("FIELD1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("FIELD2", Schema.OPTIONAL_STRING_SCHEMA);

    ensureSource(
        DataSource.DataSourceType.KTABLE, "new_table", "new_topic",
        "new_ksql_topic", schema);

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "SHOW TABLES EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        new SourceDescription(
            ksqlEngine.getMetaStore().getSource("TEST_TABLE"),
            true, "JSON", Collections.emptyList(), Collections.emptyList(),
            kafkaTopicClient),
        new SourceDescription(
            ksqlEngine.getMetaStore().getSource("new_table"),
            true, "JSON", Collections.emptyList(), Collections.emptyList(),
            kafkaTopicClient)));
  }

  @Test
  public void shouldShowQueriesExtended() {
    // Given:
    final Map<String, Object> overriddenProperties =
        Collections.singletonMap("ksql.streams.auto.offset.reset", "earliest");

    final List<PersistentQueryMetadata> queryMetadata = createQueries(
        "CREATE STREAM test_describe_1 AS SELECT * FROM test_stream;" +
            "CREATE STREAM test_describe_2 AS SELECT * FROM test_stream;", overriddenProperties);

    // When:
    final QueryDescriptionList descriptionList = makeSingleRequest(
        "SHOW QUERIES EXTENDED;", QueryDescriptionList.class);

    // Then:
    assertThat(descriptionList.getQueryDescriptions(), containsInAnyOrder(
        QueryDescription.forQueryMetadata(queryMetadata.get(0)),
        QueryDescription.forQueryMetadata(queryMetadata.get(1))));
  }

  @Test
  public void shouldDescribeStatement() {
    // Given:
    final List<RunningQuery> queries = createRunningQueries(
        "CREATE STREAM described_stream AS SELECT * FROM test_stream;"
            + "CREATE STREAM down_stream AS SELECT * FROM described_stream;",
        Collections.emptyMap());

    // When:
    final SourceDescriptionEntity description = makeSingleRequest(
        "DESCRIBE DESCRIBED_STREAM;", SourceDescriptionEntity.class);

    // Then:
    final SourceDescription expectedDescription = new SourceDescription(
        ksqlEngine.getMetaStore().getSource("DESCRIBED_STREAM"), false, "JSON",
        Collections.singletonList(queries.get(1)), Collections.singletonList(queries.get(0)), null);

    assertThat(description.getSourceDescription(), is(expectedDescription));
  }

  @Test
  public void shouldListStreamsStatement() {
    // When:
    final StreamsList streamsList = makeSingleRequest("LIST STREAMS;", StreamsList.class);

    // Then:
    assertThat(streamsList.getStreams(), contains(sourceStream("TEST_STREAM")));
  }

  @Test
  public void shouldListTablesStatement() {
    // When:
    final TablesList tablesList = makeSingleRequest("LIST TABLES;", TablesList.class);

    // Then:
    assertThat(tablesList.getTables(), contains(sourceTable("TEST_TABLE")));
  }

  @Test
  public void shouldFailForIncorrectCSASStatementResultType() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM s1 AS SELECT * FROM test_table;", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage(), is(
        "Invalid result type. Your SELECT query produces a TABLE. " +
            "Please use CREATE TABLE AS SELECT statement instead.\n"
            + "Statement: "
            + "CREATE STREAM s1 AS SELECT * FROM test_table;"));
  }

  @Test
  public void shouldFailForIncorrectCSASStatementResultTypeWithGroupBy() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM s2 AS SELECT S2_F1, count(S2_F1) FROM test_stream group by s2_f1;",
        Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage(), is(
        "Invalid result type. Your SELECT query produces a TABLE. " +
            "Please use CREATE TABLE AS SELECT statement instead.\n"
            + "Statement: "
            + "CREATE STREAM s2 AS SELECT S2_F1, count(S2_F1) FROM test_stream group by s2_f1;"));
  }

  @Test
  public void shouldFailForIncorrectCTASStatementResultType() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE TABLE s1 AS SELECT * FROM test_stream;", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage(), containsString(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead.\n"
            + "Statement: "
            + "CREATE TABLE s1 AS SELECT * FROM test_stream;"));
  }

  @Test
  public void shouldFailForIncorrectDropStreamStatement() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DROP TABLE test_stream;", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage().toLowerCase(),
        is("incompatible data source type is stream, but statement was drop table"));
  }

  @Test
  public void shouldFailForIncorrectDropTableStatement() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DROP STREAM test_table;", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage().toLowerCase(),
        is("incompatible data source type is table, but statement was drop stream"));
  }

  @Test
  public void shouldFailCreateTableWithInferenceWithUnknownKey() {
    // When:
    final KsqlErrorMessage response = makeFailingRequest(
        "CREATE TABLE orders WITH (KAFKA_TOPIC='orders-topic', "
            + "VALUE_FORMAT = 'avro', KEY = 'unknownField');",
        Code.BAD_REQUEST);

    // Then:
    assertThat(response, instanceOf(KsqlStatementErrorMessage.class));
    assertThat(response.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
  }

  @Test
  public void shouldFailBareQuery() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "SELECT * FROM test_table;", Code.BAD_REQUEST);

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_QUERY_ENDPOINT));
  }

  @Test
  public void shouldFailPrintTopic() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest("PRINT 'orders-topic';", Code.BAD_REQUEST);

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_QUERY_ENDPOINT));
  }

  @Test
  public void shouldDistributePersistentQuery() {
    // When:
    makeSingleRequest(
        "CREATE STREAM S AS SELECT * FROM test_stream;", CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        eq("CREATE STREAM S AS SELECT * FROM test_stream;"), isA(CreateStreamAsSelect.class),
        any(), any());
  }

  @Test
  public void shouldDistributeWithConfig() {
    // When:
    makeSingleRequest(VALID_EXECUTABLE_REQUEST, KsqlEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(any(), any(),
        eq(ksqlConfig), eq(VALID_EXECUTABLE_REQUEST.getStreamsProperties()));
  }

  @Test
  public void shouldReturnStatusEntityFromPersistentQuery() {
    // When:
    final CommandStatusEntity result = makeSingleRequest(
        "CREATE STREAM S AS SELECT * FROM test_stream;", CommandStatusEntity.class);

    // Then:
    assertThat(result, is(new CommandStatusEntity(
        "CREATE STREAM S AS SELECT * FROM test_stream;",
        commandStatus.getCommandId(), commandStatus.getStatus(), 0L)));
  }

  @Test
  public void shouldFailIfCreateStatementMissingKafkaTopicName() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='JSON');",
        Code.BAD_REQUEST);

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        is("Corresponding Kafka topic (KAFKA_TOPIC) should be set in WITH clause."));
    assertThat(((KsqlStatementErrorMessage) result).getStatementText(),
        is("CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldReturnBadStatementIfStatementFailsValidation() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DESCRIBE i_do_not_exist;",
        Code.BAD_REQUEST);

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(((KsqlStatementErrorMessage) result).getStatementText(),
        is("DESCRIBE i_do_not_exist;"));
  }

  @Test
  public void shouldDistributeCreateStatementEvenIfTopicDoesNotExist() {
    // When:
    makeSingleRequest(
        "CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='JSON', KAFKA_TOPIC='unknown');",
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(any(), any(), any(), any());
  }

  @Test
  public void shouldDistributeAvoCreateStatementWithColumns() {
    // When:
    makeSingleRequest(
        "CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders');",
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        eq("CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders');"),
        isA(CreateStream.class), any(), any());
  }

  @Test
  public void shouldDistributeAvroCreateStatementWithInferredColumns() {
    // When:
    makeSingleRequest(
        "CREATE TABLE orders WITH (KAFKA_TOPIC='orders-topic', VALUE_FORMAT='avro', KEY='orderid');",
        CommandStatusEntity.class);

    // Then:
    final String ksqlWithSchema =
        "CREATE TABLE ORDERS " +
            "(ORDERTIME BIGINT, ORDERID BIGINT, ITEMID STRING, ORDERUNITS DOUBLE, " +
            "ARRAYCOL ARRAY<DOUBLE>, MAPCOL MAP<VARCHAR, DOUBLE>) " +
            "WITH (KAFKA_TOPIC='orders-topic', VALUE_FORMAT='avro', " +
            "AVRO_SCHEMA_ID='1', KEY='orderid');";

    verify(commandStore).enqueueCommand(eq(ksqlWithSchema), any(), any(), any());
  }

  @Test
  public void shouldFailWhenAvroSchemaCanNotBeDetermined() {
    // Given:
    givenTopicExists("topicWithUnknownSchema");

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM S WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='topicWithUnknownSchema');",
        Code.BAD_REQUEST);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        is("Schema registry fetch for topic topicWithUnknownSchema request failed.\n\n"
            + "Caused by: No schema registered under subject!"));
  }

  //@Test
  public void shouldFailWhenAvroSchemaCanNotBeEvolved() {
    // Given:
    givenAvroSchemaNotEvolveable("S1");

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM S1 WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM test_stream;",
        Code.BAD_REQUEST);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        containsString("Cannot register avro schema for S1 as the schema registry rejected it"));
  }

  @Test
  public void shouldFailMultipleStatementsAtomically() {
    // When:
    makeFailingRequest(
        "CREATE STREAM S AS SELECT * FROM test_stream; "
            + "CREATE STREAM S2 AS SELECT * FROM S;"
            + "CREATE STREAM S2 AS SELECT * FROM S;", // <-- duplicate will fail.
        Code.BAD_REQUEST
    );

    // Then:
    verify(commandStore, never()).enqueueCommand(any(), any(), any(), any());
  }

  @Test
  public void shouldDistributeTerminateQuery() {
    // Given:
    final PersistentQueryMetadata queryMetadata = createQuery(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        Collections.emptyMap());

    final String terminateSql = "TERMINATE " + queryMetadata.getQueryId() + ";";

    // When:
    final CommandStatusEntity result = makeSingleRequest(terminateSql, CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(eq(terminateSql), isA(TerminateQuery.class), any(), any());
    assertThat(result.getStatementText(), is(terminateSql));
  }

  @Test
  public void shouldDistributeTerminateQueryWithoutValidation() {
    // Why? Because currently if the server receives a single request containing two statements:
    // `CREATE STREAM FOO AS blah;`
    // `TERMINATE csas_foo_0;`
    // Then its possible that the terminate line is valid, in that it will terminate the query
    // started by the first line, but the server can no validate this as the CSAS may not have
    // be actioned by the StatementExecutor yet.

    // Given:
    final String terminateSql = "TERMINATE some_id;";

    // When:
    final CommandStatusEntity result = makeSingleRequest(terminateSql, CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(eq(terminateSql), isA(TerminateQuery.class), any(), any());
    assertThat(result.getStatementText(), is(terminateSql));
  }

  @Test
  public void shouldExplainQueryStatement() {
    // Given:
    final String ksqlQueryString = "SELECT * FROM test_stream;";
    final String ksqlString = "EXPLAIN " + ksqlQueryString;

    // When:
    final QueryDescriptionEntity query = makeSingleRequest(
        ksqlString, QueryDescriptionEntity.class);

    // Then:
    validateQueryDescription(ksqlQueryString, Collections.emptyMap(), query);
  }

  //@Test
  public void shouldExplainCreateAsSelectStatement() {
    // Given:
    final String ksqlQueryString = "CREATE STREAM S3 AS SELECT * FROM test_stream;";
    final String ksqlString = "EXPLAIN " + ksqlQueryString;

    // When:
    final QueryDescriptionEntity query = makeSingleRequest(
        ksqlString, QueryDescriptionEntity.class);

    // Then:
    assertThat("Should not have registered the source",
        metaStore.getSource("S3"), is(nullValue()));

    validateQueryDescription(ksqlQueryString, Collections.emptyMap(), query);
  }

  @Test
  public void shouldExplainQueryId() {
    // Given:
    final Map<String, Object> overriddenProperties =
        Collections.singletonMap("ksql.streams.auto.offset.reset", "earliest");

    final PersistentQueryMetadata queryMetadata = createQuery(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        overriddenProperties);

    // When:
    final QueryDescriptionEntity query = makeSingleRequest(
        "EXPLAIN " + queryMetadata.getQueryId() + ";", QueryDescriptionEntity.class);

    // Then:
    validateQueryDescription(queryMetadata, overriddenProperties, query);
  }

  @Test
  public void shouldReportErrorOnNonQueryExplain() {
    // Given:
    final String ksqlQueryString = "SHOW TOPICS;";
    final String ksqlString = "EXPLAIN " + ksqlQueryString;

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        ksqlString, Code.BAD_REQUEST);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(), is("The provided statement does not run a ksql query"));
  }

  @Test
  public void shouldReturn5xxOnSystemError() {
    // Given:
    givenMockEngine();

    when(ksqlEngine.parseStatements(anyString()))
        .thenThrow(new RuntimeException("internal error"));

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        Code.INTERNAL_SERVER_ERROR);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(result.getMessage(), containsString("internal error"));
  }

  @Test
  public void shouldReturn5xxOnStatementSystemError() {
    // Given:
    final String ksqlString = "CREATE STREAM test_explain AS SELECT * FROM test_stream;";
    givenMockEngine();

    when(ksqlEngine.tryExecute(anyList(), any(), any()))
        .thenThrow(new RuntimeException("internal error"));

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        ksqlString, Code.INTERNAL_SERVER_ERROR);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(result.getMessage(), containsString("internal error"));
  }

  @Test
  public void shouldFailOnSetProperty() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "SET 'auto.offset.reset' = 'earliest';", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage(), is(
        "SET and UNSET commands are not supported on the REST API. "
            + "Pass properties via the 'streamsProperties' field"));
  }

  @Test
  public void shouldFailOnUnsetProperty() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "UNSET 'auto.offset.reset';", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getMessage(), is(
        "SET and UNSET commands are not supported on the REST API. "
            + "Pass properties via the 'streamsProperties' field"));
  }

  @Test
  public void shouldFailIfReachedActivePersistentQueriesLimit() {
    // Given:
    givenKsqlConfigWith(
        ImmutableMap.of(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 3));

    givenMockEngine();

    when(ksqlEngine.numberOfPersistentQueries()).thenReturn(3L);

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM new_stream AS SELECT * FROM test_stream;", Code.BAD_REQUEST);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_REQUEST));
    assertThat(result.getMessage(),
        containsString("would cause the number of active, persistent queries "
            + "to exceed the configured limit"));
  }

  @Test
  public void shouldFailAllCommandsIfWouldReachActivePersistentQueriesLimit() {
    // Given:
    givenKsqlConfigWith(
        ImmutableMap.of(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 3));

    final String ksqlString = "CREATE STREAM new_stream AS SELECT * FROM test_stream;"
        + "CREATE STREAM another_stream AS SELECT * FROM test_stream;";
    givenMockEngine();

    when(ksqlEngine.numberOfPersistentQueries()).thenReturn(2L);

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        ksqlString, Code.BAD_REQUEST);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_REQUEST));
    assertThat(result.getMessage(),
        containsString("would cause the number of active, persistent queries "
            + "to exceed the configured limit"));

    verify(commandStore, never()).enqueueCommand(any(), any(), any(), any());
  }

  @Test
  public void shouldListPropertiesWithOverrides() {
    // Given:
    final Map<String, Object> overrides = Collections.singletonMap("auto.offset.reset", "latest");

    // When:
    final PropertiesList props = makeSingleRequest(
        new KsqlRequest("list properties;", overrides, null), PropertiesList.class);

    // Then:
    assertThat(props.getProperties().get("ksql.streams.auto.offset.reset"), is("latest"));
    assertThat(props.getOverwrittenProperties(), hasItem("ksql.streams.auto.offset.reset"));
  }

  @Test
  public void shouldListPropertiesWithNoOverrides() {
    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getOverwrittenProperties(), is(empty()));
  }

  @Test
  public void shouldListDefaultKsqlProperty() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        hasItem(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG));
  }

  @Test
  public void shouldListServerOverriddenKsqlProperty() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/other")
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        not(hasItem(containsString(StreamsConfig.STATE_DIR_CONFIG))));
  }

  @Test
  public void shouldListDefaultStreamProperty() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, STATE_CLEANUP_DELAY_MS_DEFAULT)
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        hasItem(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG));
  }

  @Test
  public void shouldListServerOverriddenStreamProperty() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, STATE_CLEANUP_DELAY_MS_DEFAULT + 1)
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        not(hasItem(containsString(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG))));
  }

  @Test
  public void shouldListDefaultConsumerConfig() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, FETCH_MIN_BYTES_DEFAULT)
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        hasItem(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
  }

  @Test
  public void shouldListServerOverriddenConsumerConfig() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, FETCH_MIN_BYTES_DEFAULT + 1)
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        not(hasItem(containsString(ConsumerConfig.FETCH_MIN_BYTES_CONFIG))));
  }

  @Test
  public void shouldListDefaultProducerConfig() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG,
            BUFFER_MEMORY_DEFAULT)
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        hasItem(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX
            + ProducerConfig.BUFFER_MEMORY_CONFIG));
  }

  @Test
  public void shouldListServerOverriddenProducerConfig() {
    // Given:
    givenKsqlConfigWith(ImmutableMap.<String, Object>builder()
        .put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG,
            BUFFER_MEMORY_DEFAULT + 1)
        .build());

    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getDefaultProperties(),
        not(hasItem(containsString(ProducerConfig.BUFFER_MEMORY_CONFIG))));
  }

  @Test
  public void shouldNotIncludeSslPropertiesInListPropertiesOutput() {
    // When:
    final PropertiesList props = makeSingleRequest("list properties;", PropertiesList.class);

    // Then:
    assertThat(props.getProperties().keySet(),
        not(hasItems(KsqlConfig.SSL_CONFIG_NAMES.toArray(new String[0]))));
  }

  @Test
  public void shouldNotWaitIfNoCommandSequenceNumberSpecified() throws Exception {
    // When:
    makeSingleRequestWithSequenceNumber("list properties;", null, PropertiesList.class);

    // Then:
    verify(commandStore, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    makeSingleRequestWithSequenceNumber("list properties;", 2L, PropertiesList.class);

    // Then:
    verify(commandStore).ensureConsumedPast(eq(2L), any());
  }

  @Test
  public void shouldReturnServiceUnavailableIfTimeoutWaitingForCommandSequenceNumber()
      throws Exception {
    // Given:
    doThrow(new TimeoutException("timed out!"))
        .when(commandStore).ensureConsumedPast(anyLong(), any());

    // When:
    final KsqlErrorMessage result =
        makeFailingRequestWithSequenceNumber("list properties;", 2L, Code.SERVICE_UNAVAILABLE);

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT));
    assertThat(result.getMessage(), is("timed out!"));
  }

  @Test
  public void shouldUpdateTheLastRequestTime() {
    // When:
    ksqlResource.handleKsqlStatements(VALID_EXECUTABLE_REQUEST);

    // Then:
    verify(activenessRegistrar).updateLastRequestTime();
  }

  @SuppressWarnings("SameParameterValue")
  private SourceInfo.Table sourceTable(final String name) {
    final KsqlTable table = (KsqlTable) ksqlEngine.getMetaStore().getSource(name);
    return new SourceInfo.Table(table);
  }

  @SuppressWarnings("SameParameterValue")
  private SourceInfo.Stream sourceStream(final String name) {
    final KsqlStream stream = (KsqlStream) ksqlEngine.getMetaStore().getSource(name);
    return new SourceInfo.Stream(stream);
  }

  private void givenMockEngine() {
    ksqlEngine = mock(KsqlEngine.class);
    when(ksqlEngine.parseStatements(any()))
        .thenAnswer(invocation -> realEngine.parseStatements(invocation.getArgument(0)));
    setUpKsqlResource();
  }

  private List<PersistentQueryMetadata> createQueries(
      final String sql,
      final Map<String, Object> overriddenProperties) {
    return ksqlEngine.execute(sql, ksqlConfig, overriddenProperties)
        .stream()
        .map(PersistentQueryMetadata.class::cast)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("SameParameterValue")
  private PersistentQueryMetadata createQuery(
      final String ksqlQueryString,
      final Map<String, Object> overriddenProperties) {
    return createQueries(ksqlQueryString, overriddenProperties).get(0);
  }

  @SuppressWarnings("SameParameterValue")
  private List<RunningQuery> createRunningQueries(
      final String sql,
      final Map<String, Object> overriddenProperties) {

    return createQueries(sql, overriddenProperties)
        .stream()
        .map(md -> new RunningQuery(
            md.getStatementString(),
            md.getSinkNames(),
            new EntityQueryId(md.getQueryId())))
        .collect(Collectors.toList());
  }

  private KsqlErrorMessage makeFailingRequest(final String ksql, final Code errorCode) {
    return makeFailingRequestWithSequenceNumber(ksql, null, errorCode);
  }

  private KsqlErrorMessage makeFailingRequestWithSequenceNumber(
      final String ksql,
      final Long seqNum,
      final Code errorCode) {
    return makeFailingRequest(new KsqlRequest(ksql, Collections.emptyMap(), seqNum), errorCode);
  }

  private KsqlErrorMessage makeFailingRequest(final KsqlRequest ksqlRequest, final Code errorCode) {
    try {
      final Response response = ksqlResource.handleKsqlStatements(ksqlRequest);
      assertThat(response.getStatus(), is(errorCode.getCode()));
      assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
      return (KsqlErrorMessage) response.getEntity();
    } catch (KsqlRestException e) {
      return (KsqlErrorMessage) e.getResponse().getEntity();
    }
  }

  private <T extends KsqlEntity> T makeSingleRequest(
      final String sql,
      final Class<T> expectedEntityType) {
    return makeSingleRequestWithSequenceNumber(sql, null, expectedEntityType);
  }

  private <T extends KsqlEntity> T makeSingleRequestWithSequenceNumber(
      final String sql,
      final Long seqNum,
      final Class<T> expectedEntityType) {
    return makeSingleRequest(
        new KsqlRequest(sql, Collections.emptyMap(), seqNum), expectedEntityType);
  }

  private <T extends KsqlEntity> T makeSingleRequest(
      final KsqlRequest ksqlRequest,
      final Class<T> expectedEntityType) {

    final List<T> entities = makeMultipleRequest(ksqlRequest, expectedEntityType);
    assertThat(entities, hasSize(1));
    return entities.get(0);
  }

  private <T extends KsqlEntity> List<T> makeMultipleRequest(
      final KsqlRequest ksqlRequest,
      final Class<T> expectedEntityType) {

    final Response response = ksqlResource.handleKsqlStatements(ksqlRequest);

    final Object entity = response.getEntity();
    assertThat(String.valueOf(entity), response.getStatus(),
        is(Response.Status.OK.getStatusCode()));
    assertThat(entity, instanceOf(KsqlEntityList.class));
    final KsqlEntityList entityList = (KsqlEntityList) entity;
    entityList.forEach(e -> assertThat(e, instanceOf(expectedEntityType)));
    return entityList.stream()
        .map(expectedEntityType::cast)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("SameParameterValue")
  private void validateQueryDescription(
      final String ksqlQueryString,
      final Map<String, Object> overriddenProperties,
      final KsqlEntity entity) {
    final QueryMetadata queryMetadata = ksqlEngine
        .execute(ksqlQueryString, ksqlConfig, overriddenProperties).get(0);

    validateQueryDescription(queryMetadata, overriddenProperties, entity);
  }

  private static void validateQueryDescription(
      final QueryMetadata queryMetadata,
      final Map<String, Object> overriddenProperties,
      final KsqlEntity entity) {
    assertThat(entity, instanceOf(QueryDescriptionEntity.class));
    final QueryDescriptionEntity queryDescriptionEntity = (QueryDescriptionEntity) entity;
    final QueryDescription queryDescription = queryDescriptionEntity.getQueryDescription();
    assertThat(queryDescription.getFields(), is(
        EntityUtil.buildSourceSchemaEntity(queryMetadata.getOutputNode().getSchema())));
    assertThat(queryDescription.getOverriddenProperties(), is(overriddenProperties));
  }

  private void setUpKsqlResource() {
    ksqlResource = new KsqlResource(
        ksqlConfig, ksqlEngine, serviceContext, commandStore, DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT,
        activenessRegistrar);
  }

  private void givenKsqlConfigWith(final Map<String, Object> additionalConfig) {
    final Map<String, Object> config = ksqlRestConfig.getKsqlConfigProperties();
    config.putAll(additionalConfig);
    ksqlConfig = new KsqlConfig(config);

    setUpKsqlResource();
  }

  private void addTestTopicAndSources() {
    final Schema schema1 = SchemaBuilder.struct().field("S1_F1", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    ensureSource(
        DataSource.DataSourceType.KTABLE,
        "TEST_TABLE", "KAFKA_TOPIC_1", "KSQL_TOPIC_1", schema1);
    final Schema schema2 = SchemaBuilder.struct().field("S2_F1", Schema.OPTIONAL_STRING_SCHEMA);
    ensureSource(
        DataSource.DataSourceType.KSTREAM,
        "TEST_STREAM", "KAFKA_TOPIC_2", "KSQL_TOPIC_2", schema2);
    givenTopicExists("orders-topic");
  }

  private void ensureSource(
      final DataSource.DataSourceType type,
      final String sourceName,
      final String topicName,
      final String ksqlTopicName,
      final Schema schema) {
    final MetaStore metaStore = ksqlEngine.getMetaStore();
    if (metaStore.getTopic(ksqlTopicName) != null) {
      return;
    }

    final KsqlTopic ksqlTopic = new KsqlTopic(ksqlTopicName, topicName, new KsqlJsonTopicSerDe(), false);
    givenTopicExists(topicName);
    metaStore.putTopic(ksqlTopic);
    if (type == DataSource.DataSourceType.KSTREAM) {
      metaStore.putSource(
          new KsqlStream<>(
              "statementText", sourceName, schema, schema.fields().get(0),
              new MetadataTimestampExtractionPolicy(), ksqlTopic, Serdes.String()));
    }
    if (type == DataSource.DataSourceType.KTABLE) {
      metaStore.putSource(
          new KsqlTable<>(
              "statementText", sourceName, schema, schema.fields().get(0),
              new MetadataTimestampExtractionPolicy(), ksqlTopic, "statestore", Serdes.String()));
    }
  }

  private static Properties getDefaultKsqlConfig() {
    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("commit.interval.ms", 0);
    configMap.put("cache.max.bytes.buffering", 0);
    configMap.put("auto.offset.reset", "earliest");
    configMap.put("ksql.command.topic.suffix", "commands");
    configMap.put(RestConfig.LISTENERS_CONFIG, "http://localhost:8088");

    final Properties properties = new Properties();
    properties.putAll(configMap);

    return properties;
  }

  private static void registerSchema(final SchemaRegistryClient schemaRegistryClient)
      throws IOException, RestClientException {
    final String ordersAveroSchemaStr = "{"
        + "\"namespace\": \"kql\","
        + " \"name\": \"orders\","
        + " \"type\": \"record\","
        + " \"fields\": ["
        + "     {\"name\": \"ordertime\", \"type\": \"long\"},"
        + "     {\"name\": \"orderid\",  \"type\": \"long\"},"
        + "     {\"name\": \"itemid\", \"type\": \"string\"},"
        + "     {\"name\": \"orderunits\", \"type\": \"double\"},"
        + "     {\"name\": \"arraycol\", \"type\": {\"type\": \"array\", \"items\": \"double\"}},"
        + "     {\"name\": \"mapcol\", \"type\": {\"type\": \"map\", \"values\": \"double\"}}"
        + " ]"
        + "}";
    final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    final org.apache.avro.Schema avroSchema = parser.parse(ordersAveroSchemaStr);
    schemaRegistryClient.register("orders-topic" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX,
        avroSchema);
  }

  @SuppressWarnings("SameParameterValue")
  private void givenAvroSchemaNotEvolveable(final String topicName) {
    final org.apache.avro.Schema schema = org.apache.avro.Schema.create(Type.INT);

    try {
      schemaRegistryClient.register(topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  private void givenTopicExists(final String name) {
    kafkaTopicClient.createTopic(name, 1, (short) 1);
  }
}
