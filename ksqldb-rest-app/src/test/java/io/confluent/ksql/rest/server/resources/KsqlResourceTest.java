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

package io.confluent.ksql.rest.server.resources;

import static io.confluent.ksql.parser.ParserMatchers.configured;
import static io.confluent.ksql.parser.ParserMatchers.preparedStatementText;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_FORBIDDEN_KAFKA_ACCESS;
import static io.confluent.ksql.rest.entity.ClusterTerminateRequest.DELETE_TOPIC_LIST_PROP;
import static io.confluent.ksql.rest.entity.CommandId.Action.CREATE;
import static io.confluent.ksql.rest.entity.CommandId.Action.DROP;
import static io.confluent.ksql.rest.entity.CommandId.Action.EXECUTE;
import static io.confluent.ksql.rest.entity.CommandId.Type.STREAM;
import static io.confluent.ksql.rest.entity.CommandId.Type.TABLE;
import static io.confluent.ksql.rest.entity.CommandId.Type.TOPIC;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers.statement;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatementErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.MutableFunctionRegistry;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.PropertiesList.Property;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionFactory;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStatusFuture;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.computation.QueuedCommandStatus;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.FakeKafkaConsumerGroupClient;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.topic.TopicDeleteInjector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Sandbox;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema.Type;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@SuppressWarnings({"unchecked", "SameParameterValue"})
@RunWith(MockitoJUnitRunner.class)
public class KsqlResourceTest {

  private static final long STATE_CLEANUP_DELAY_MS_DEFAULT = 10 * 60 * 1000L;
  private static final int FETCH_MIN_BYTES_DEFAULT = 1;
  private static final long BUFFER_MEMORY_DEFAULT = 32 * 1024 * 1024L;
  private static final Duration DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT = Duration.ofMillis(1000);
  private static final KsqlRequest VALID_EXECUTABLE_REQUEST = new KsqlRequest(
      "CREATE STREAM S AS SELECT * FROM test_stream;",
      ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
      emptyMap(),
      0L);

  private static final LogicalSchema SINGLE_FIELD_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("val"), SqlTypes.STRING)
      .build();

  private static final ClusterTerminateRequest VALID_TERMINATE_REQUEST =
      new ClusterTerminateRequest(ImmutableList.of("Foo"));
  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("f0"), new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.STRING))
  );
  private static final PreparedStatement<CreateStream> STMT_0_WITH_SCHEMA = PreparedStatement.of(
      "sql with schema",
      new CreateStream(
          SourceName.of("bob"),
          SOME_ELEMENTS,
          false,
          true,
          CreateSourceProperties.from(ImmutableMap.of(
              "KAFKA_TOPIC", new StringLiteral("orders-topic"),
              "VALUE_FORMAT", new StringLiteral("avro")
          ))));
  private static final ConfiguredStatement<CreateStream> CFG_0_WITH_SCHEMA = ConfiguredStatement.of(
      STMT_0_WITH_SCHEMA,
      ImmutableMap.of(),
      new KsqlConfig(getDefaultKsqlConfig())
  );

  private static final PreparedStatement<CreateStream> STMT_1_WITH_SCHEMA = PreparedStatement.of(
      "other sql with schema",
      new CreateStream(
          SourceName.of("john"),
          SOME_ELEMENTS,
          false,
          true,
          CreateSourceProperties.from(ImmutableMap.of(
              "KAFKA_TOPIC", new StringLiteral("orders-topic"),
              "VALUE_FORMAT", new StringLiteral("avro")
          ))));
  private static final ConfiguredStatement<CreateStream> CFG_1_WITH_SCHEMA = ConfiguredStatement.of(
      STMT_1_WITH_SCHEMA,
      ImmutableMap.of(),
      new KsqlConfig(getDefaultKsqlConfig())
  );

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f1"), SqlTypes.STRING)
      .build();

  private static final String APPLICATION_HOST = "localhost";
  private static final int APPLICATION_PORT = 9099;
  private static final String APPLICATION_SERVER = "http://" + APPLICATION_HOST + ":" + APPLICATION_PORT;

  private KsqlConfig ksqlConfig;
  private KsqlRestConfig ksqlRestConfig;
  private FakeKafkaTopicClient kafkaTopicClient;
  private FakeKafkaConsumerGroupClient kafkaConsumerGroupClient;
  private KsqlEngine realEngine;
  private KsqlEngine ksqlEngine;
  @Mock
  private SandboxEngine sandbox;
  @Mock
  private CommandStore commandStore;
  @Mock
  private CommandRunner commandRunner;
  @Mock
  private ActivenessRegistrar activenessRegistrar;
  @Mock
  private Function<ServiceContext, Injector> schemaInjectorFactory;
  @Mock
  private Injector schemaInjector;
  @Mock
  private Injector sandboxSchemaInjector;
  @Mock
  private Function<KsqlExecutionContext, Injector> topicInjectorFactory;
  @Mock
  private Injector topicInjector;
  @Mock
  private Injector sandboxTopicInjector;
  @Mock
  private KsqlAuthorizationValidator authorizationValidator;
  @Mock
  private Producer<CommandId, Command> transactionalProducer;
  @Mock
  private Errors errorsHandler;
  @Mock
  private DenyListPropertyValidator denyListPropertyValidator;

  private KsqlResource ksqlResource;
  private SchemaRegistryClient schemaRegistryClient;
  private QueuedCommandStatus commandStatus;
  private QueuedCommandStatus commandStatus1;
  private MetaStoreImpl metaStore;
  private ServiceContext serviceContext;
  private KsqlSecurityContext securityContext;

  private String streamName;

  @Before
  public void setUp() throws IOException, RestClientException {
    commandStatus = new QueuedCommandStatus(
        0, new CommandStatusFuture(new CommandId(TOPIC, "whateva", CREATE)));

    commandStatus1 = new QueuedCommandStatus(
        1, new CommandStatusFuture(new CommandId(TABLE, "something", DROP)));

    final QueuedCommandStatus commandStatus2 = new QueuedCommandStatus(
        2, new CommandStatusFuture(new CommandId(STREAM, "something", EXECUTE)));

    kafkaTopicClient = new FakeKafkaTopicClient();
    kafkaConsumerGroupClient = new FakeKafkaConsumerGroupClient();
    serviceContext = TestServiceContext.create(kafkaTopicClient, kafkaConsumerGroupClient);
    schemaRegistryClient = serviceContext.getSchemaRegistryClient();
    registerSchema(schemaRegistryClient);
    ksqlRestConfig = new KsqlRestConfig(getDefaultKsqlConfig());
    ksqlConfig = new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties());

    MutableFunctionRegistry fnRegistry = new InternalFunctionRegistry();
    UserFunctionLoader.newInstance(ksqlConfig, fnRegistry, ".").load();
    metaStore = new MetaStoreImpl(fnRegistry);

    realEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore
    );

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    when(commandRunner.getCommandQueue()).thenReturn(commandStore);
    when(commandStore.createTransactionalProducer())
        .thenReturn(transactionalProducer);

    ksqlEngine = realEngine;
    when(sandbox.getMetaStore()).thenAnswer(inv -> metaStore.copy());

    addTestTopicAndSources();

    when(commandStore.enqueueCommand(any(), any(), any(Producer.class)))
        .thenReturn(commandStatus)
        .thenReturn(commandStatus1)
        .thenReturn(commandStatus2);

    streamName = KsqlIdentifierTestUtil.uniqueIdentifierName();

    when(schemaInjectorFactory.apply(any())).thenReturn(sandboxSchemaInjector);
    when(schemaInjectorFactory.apply(serviceContext)).thenReturn(schemaInjector);

    when(topicInjectorFactory.apply(any())).thenReturn(sandboxTopicInjector);
    when(topicInjectorFactory.apply(ksqlEngine)).thenReturn(topicInjector);

    when(sandboxSchemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    when(sandboxTopicInjector.inject(any()))
        .thenAnswer(inv -> inv.getArgument(0));
    when(topicInjector.inject(any()))
        .thenAnswer(inv -> inv.getArgument(0));

    when(errorsHandler.generateResponse(any(), any())).thenAnswer(new Answer<EndpointResponse>() {
      @Override
      public EndpointResponse answer(final InvocationOnMock invocation) throws Throwable {
        final Object[] args = invocation.getArguments();
        return (EndpointResponse) args[1];
      }
    });

    setUpKsqlResource();
  }

  @After
  public void tearDown() {
    realEngine.close();
    serviceContext.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnConfigureIfAppServerNotSet() {
    // Given:
    final KsqlConfig configNoAppServer = new KsqlConfig(ImmutableMap.of());

    // When:
    ksqlResource.configure(configNoAppServer);
  }

  @Test
  public void shouldThrowOnHandleStatementIfNotConfigured() {
    // Given:
    ksqlResource = new KsqlResource(
        ksqlEngine,
        commandRunner,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT,
        activenessRegistrar,
        (ec, sc) -> InjectorChain.of(
            schemaInjectorFactory.apply(sc),
            topicInjectorFactory.apply(ec),
            new TopicDeleteInjector(ec, sc)),
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator
    );

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> ksqlResource.handleKsqlStatements(
            securityContext,
            new KsqlRequest("query", emptyMap(), emptyMap(), null)
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(CoreMatchers.is(SERVICE_UNAVAILABLE.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(Matchers.is("Server initializing"))));
  }

  @Test
  public void shouldThrowOnHandleTerminateIfNotConfigured() {
    // Given:
    ksqlResource = new KsqlResource(
        ksqlEngine,
        commandRunner,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT,
        activenessRegistrar,
        (ec, sc) -> InjectorChain.of(
            schemaInjectorFactory.apply(sc),
            topicInjectorFactory.apply(ec),
            new TopicDeleteInjector(ec, sc)),
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator
    );

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> ksqlResource.terminateCluster(
            securityContext,
            new ClusterTerminateRequest(ImmutableList.of(""))
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(CoreMatchers.is(SERVICE_UNAVAILABLE.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(Matchers.is("Server initializing"))));
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
        new SimpleFunctionInfo("TRIM", FunctionType.SCALAR, 
            FunctionCategory.STRING),
        new SimpleFunctionInfo("TOPK", FunctionType.AGGREGATE,
            FunctionCategory.AGGREGATE),
        new SimpleFunctionInfo("MAX", FunctionType.AGGREGATE,
            FunctionCategory.AGGREGATE)
    ));
  }

  @Test
  public void shouldShowStreamsExtended() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("FIELD1"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("FIELD2"), SqlTypes.STRING)
        .build();

    givenSource(
        DataSourceType.KSTREAM, "new_stream", "new_topic",
        schema);

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "SHOW STREAMS EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        SourceDescriptionFactory.create(
            ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_STREAM")),
            true, Collections.emptyList(), Collections.emptyList(),
            Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_2")),
            Collections.emptyList()),
        SourceDescriptionFactory.create(
            ksqlEngine.getMetaStore().getSource(SourceName.of("new_stream")),
            true, Collections.emptyList(), Collections.emptyList(),
            Optional.of(kafkaTopicClient.describeTopic("new_topic")),
            Collections.emptyList()))
    );
  }

  @Test
  public void shouldShowTablesExtended() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("FIELD1"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("FIELD2"), SqlTypes.STRING)
        .build();

    givenSource(
        DataSourceType.KTABLE, "new_table", "new_topic",
        schema);

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "SHOW TABLES EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        SourceDescriptionFactory.create(
            ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_TABLE")),
            true, Collections.emptyList(), Collections.emptyList(),
            Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_1")),
            Collections.emptyList()),
        SourceDescriptionFactory.create(
            ksqlEngine.getMetaStore().getSource(SourceName.of("new_table")),
            true, Collections.emptyList(), Collections.emptyList(),
            Optional.of(kafkaTopicClient.describeTopic("new_topic")),
            Collections.emptyList()))
    );
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

    final Map<KsqlHostInfoEntity, KsqlConstants.KsqlQueryStatus> queryHostState =
        ImmutableMap.of(new KsqlHostInfoEntity(APPLICATION_HOST, APPLICATION_PORT), KsqlConstants.KsqlQueryStatus.RUNNING);
    // Then:
    assertThat(descriptionList.getQueryDescriptions(), containsInAnyOrder(
        QueryDescriptionFactory.forQueryMetadata(queryMetadata.get(0), queryHostState),
        QueryDescriptionFactory.forQueryMetadata(queryMetadata.get(1), queryHostState)));
  }

  @Test
  public void shouldDescribeStatement() {
    // Given:
    final List<RunningQuery> queries = createRunningQueries(
        "CREATE STREAM described_stream AS SELECT * FROM test_stream;"
            + "CREATE STREAM down_stream AS SELECT * FROM described_stream;",
        emptyMap());

    // When:
    final SourceDescriptionEntity description = makeSingleRequest(
        "DESCRIBE DESCRIBED_STREAM;", SourceDescriptionEntity.class);

    // Then:
    final SourceDescription expectedDescription = SourceDescriptionFactory.create(
        ksqlEngine.getMetaStore().getSource(SourceName.of("DESCRIBED_STREAM")),
        false,
        Collections.singletonList(queries.get(1)),
        Collections.singletonList(queries.get(0)),
        Optional.empty(),
        Collections.emptyList());

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
  public void shouldExecuteMaxNumberPersistentQueries() {
    // Given:
    final String sql = "CREATE STREAM S AS SELECT * FROM test_stream;";
    givenKsqlConfigWith(ImmutableMap.of(
        KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, "1"
    ));
    setUpKsqlResource();


    // When:
    makeSingleRequest(sql, CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(sql))),
        any(Producer.class)
    );
  }

  @Test
  public void shouldFailForIncorrectCSASStatementResultType() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE STREAM s1 AS SELECT * FROM test_table;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead."))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "CREATE STREAM s1 AS SELECT * FROM test_table;"))));
  }

  @Test
  public void shouldFailForIncorrectCSASStatementResultTypeWithGroupBy() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE STREAM s2 AS SELECT S2_F1, count(S2_F1) FROM test_stream group by s2_f1;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is(
        "Invalid result type. Your SELECT query produces a TABLE. "
            + "Please use CREATE TABLE AS SELECT statement instead."))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "CREATE STREAM s2 AS SELECT S2_F1, count(S2_F1) FROM test_stream group by s2_f1;"))));
  }

  @Test
  public void shouldFailForIncorrectCTASStatementResultType() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE TABLE s1 AS SELECT * FROM test_stream;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is(
        "Invalid result type. Your SELECT query produces a STREAM. "
            + "Please use CREATE STREAM AS SELECT statement instead."))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "CREATE TABLE s1 AS SELECT * FROM test_stream;"))));
  }

  @Test
  public void shouldFailForIncorrectDropStreamStatement() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DROP TABLE test_stream;", BAD_REQUEST.code());

    // Then:
    assertThat(result.getMessage().toLowerCase(),
        is("incompatible data source type is stream, but statement was drop table"));
  }

  @Test
  public void shouldFailForIncorrectDropTableStatement() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DROP STREAM test_table;", BAD_REQUEST.code());

    // Then:
    assertThat(result.getMessage().toLowerCase(),
        is("incompatible data source type is table, but statement was drop stream"));
  }

  @Test
  public void shouldFailBarePushQuery() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("SELECT * FROM test_table EMIT CHANGES;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(is(
        "The following statement types should be issued to the websocket endpoint '/query':"
            + System.lineSeparator()
            + "\t* PRINT"
            + System.lineSeparator()
            + "\t* SELECT"))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "SELECT * FROM test_table EMIT CHANGES;"))));
  }

  @Test
  public void shouldFailBarePullQuery() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("SELECT * FROM test_table;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(is(
        "The following statement types should be issued to the websocket endpoint '/query':"
            + System.lineSeparator()
            + "\t* PRINT"
            + System.lineSeparator()
            + "\t* SELECT"))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "SELECT * FROM test_table;"))));
  }

  @Test
  public void shouldFailPrintTopic() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("PRINT 'orders-topic';")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(is(
        "The following statement types should be issued to the websocket endpoint '/query':"
            + System.lineSeparator()
            + "\t* PRINT"
            + System.lineSeparator()
            + "\t* SELECT"))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "PRINT 'orders-topic';"))));
  }

  @Test
  public void shouldDistributePersistentQuery() {
    // Given:
    final String sql = "CREATE STREAM S AS SELECT * FROM test_stream;";

    // When:
    makeSingleRequest(sql, CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(sql))),
        any(Producer.class)
    );
  }

  @Test
  public void shouldDistributeWithStreamsProperties() {
    // When:
    makeSingleRequest(VALID_EXECUTABLE_REQUEST, KsqlEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithOverwrittenProperties(
            VALID_EXECUTABLE_REQUEST.getConfigOverrides()))),
        any(Producer.class)
    );
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
        BAD_REQUEST.code());

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        containsString("Missing required property \"KAFKA_TOPIC\" which has no default value."));
    assertThat(((KsqlStatementErrorMessage) result).getStatementText(),
        is("CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessIfKsqlTopicAuthorizationException() {
    // Given:
    final String errorMsg = "some error";
    when(errorsHandler.generateResponse(any(), any())).thenReturn(EndpointResponse.create()
        .status(FORBIDDEN.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, errorMsg))
        .build());
    doThrow(new KsqlTopicAuthorizationException(
        AclOperation.DELETE,
        Collections.singleton("topic"))).when(authorizationValidator)
        .checkAuthorization(any(), any(), any());

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DROP STREAM TEST_STREAM DELETE TOPIC;",
        FORBIDDEN.code());

    // Then:
    assertThat(result, is(instanceOf(KsqlErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_FORBIDDEN_KAFKA_ACCESS));
    assertThat(result.getMessage(), is(errorMsg));
  }

  @Test
  public void shouldReturnBadStatementIfStatementFailsValidation() {
    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "DESCRIBE i_do_not_exist;",
        BAD_REQUEST.code());

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(((KsqlStatementErrorMessage) result).getStatementText(),
        is("DESCRIBE i_do_not_exist;"));
  }

  @Test
  public void shouldNotDistributeCreateStatementIfTopicDoesNotExist() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='JSON', KAFKA_TOPIC='unknown');")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is("Kafka topic does not exist: unknown"))));
  }

  @Test
  public void shouldDistributeAvroCreateStatementWithColumns() {
    // When:
    makeSingleRequest(
        "CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');",
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(
            "CREATE STREAM S (foo INT) WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');"))),
        any(Producer.class)
    );
  }

  @Test
  public void shouldSupportTopicInferenceInVerification() {
    // Given:
    givenMockEngine();
    givenSource(DataSourceType.KSTREAM, "ORDERS1", "ORDERS1", SOME_SCHEMA);

    final String sql = "CREATE STREAM orders2 AS SELECT * FROM orders1;";
    final String sqlWithTopic = "CREATE STREAM orders2 WITH(kafka_topic='orders2') AS SELECT * FROM orders1;";

    final PreparedStatement<?> statementWithTopic =
        ksqlEngine.prepare(ksqlEngine.parse(sqlWithTopic).get(0));
    final ConfiguredStatement<?> configuredStatement =
        ConfiguredStatement.of(statementWithTopic, ImmutableMap.of(), ksqlConfig);

    when(sandboxTopicInjector.inject(argThat(is(configured(preparedStatementText(sql))))))
        .thenReturn((ConfiguredStatement<Statement>) configuredStatement);

    // When:
    makeRequest(sql);

    // Then:
    verify(sandbox).plan(any(SandboxedServiceContext.class), eq(configuredStatement));
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(sql))),
        any(Producer.class)
    );
  }

  @Test
  public void shouldSupportTopicInferenceInExecution() {
    // Given:
    givenMockEngine();
    givenSource(DataSourceType.KSTREAM, "ORDERS1", "ORDERS1", SOME_SCHEMA);

    final String sql = "CREATE STREAM orders2 AS SELECT * FROM orders1;";
    final String sqlWithTopic = "CREATE STREAM orders2 WITH(kafka_topic='orders2') AS SELECT * FROM orders1;";

    final PreparedStatement<?> statementWithTopic =
        ksqlEngine.prepare(ksqlEngine.parse(sqlWithTopic).get(0));
    final ConfiguredStatement<?> configured =
        ConfiguredStatement.of(statementWithTopic, ImmutableMap.of(), ksqlConfig);

    when(topicInjector.inject(argThat(is(configured(preparedStatementText(sql))))))
        .thenReturn((ConfiguredStatement<Statement>) configured);

    // When:
    makeRequest(sql);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(sqlWithTopic))),
        any()
    );
  }

  @Test
  public void shouldFailWhenTopicInferenceFailsDuringValidate() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "ORDERS1", "ORDERS1", SOME_SCHEMA);
    when(sandboxTopicInjector.inject(any()))
        .thenThrow(new KsqlStatementException("boom", "sql"));

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM orders2 AS SELECT * FROM orders1;",
        BAD_REQUEST.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(), is("boom"));
  }

  @Test
  public void shouldFailWhenTopicInferenceFailsDuringExecute() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "ORDERS1", "ORDERS1", SOME_SCHEMA);

    when(topicInjector.inject(any()))
        .thenThrow(new KsqlStatementException("boom", "some-sql"));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE STREAM orders2 AS SELECT * FROM orders1;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_BAD_STATEMENT))));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(is("boom"))));
  }

  @Test
  public void shouldSupportSchemaInference() {
    // Given:
    givenMockEngine();

    final String sql = "CREATE STREAM NO_SCHEMA WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');";

    when(sandboxSchemaInjector.inject(argThat(configured(preparedStatementText(sql)))))
        .thenReturn((ConfiguredStatement) CFG_0_WITH_SCHEMA);

    when(schemaInjector.inject(argThat(configured(preparedStatementText(sql)))))
        .thenReturn((ConfiguredStatement) CFG_1_WITH_SCHEMA);

    // When:
    makeRequest(sql);

    // Then:
    verify(sandbox).plan(any(SandboxedServiceContext.class), eq(CFG_0_WITH_SCHEMA));
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(CFG_1_WITH_SCHEMA.getStatementText()))),
        any()
    );
  }

  @Test
  public void shouldFailWhenAvroInferenceFailsDuringValidate() {
    // Given:
    when(sandboxSchemaInjector.inject(any()))
        .thenThrow(new KsqlStatementException("boom", "sql"));

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM S WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');",
        BAD_REQUEST.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(), is("boom"));
  }

  @Test
  public void shouldFailWhenAvroInferenceFailsDuringExecute() {
    // Given:
    when(sandboxSchemaInjector.inject(any()))
        .thenReturn((ConfiguredStatement) CFG_0_WITH_SCHEMA);

    when(schemaInjector.inject(any()))
        .thenThrow(new KsqlStatementException("boom", "some-sql"));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE STREAM S WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_BAD_STATEMENT))));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(is("boom"))));
  }

  @Test
  public void shouldFailIfNoSchemaAndNotInferred() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("CREATE STREAM S WITH(VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_BAD_STATEMENT))));
    assertThat(e, exceptionErrorMessage(errorMessage(is("The statement does not define any columns."))));
  }

  @Test
  public void shouldFailWhenAvroSchemaCanNotBeEvolved() {
    // Given:
    givenAvroSchemaNotEvolveable("S1");

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM S1 WITH(VALUE_FORMAT='AVRO') AS SELECT * FROM test_stream;",
        BAD_REQUEST.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        containsString("Cannot register avro schema for S1 as the schema is incompatible with the current schema version registered for the topic"));
  }

  @Test
  public void shouldWaitForLastDistributedStatementBeforeExecutingAnyNonDistributed()
      throws Exception {
    // Given:
    final String csasSql = "CREATE STREAM S AS SELECT * FROM test_stream;";

    doAnswer(executeAgainstEngine(csasSql))
        .when(commandStore)
        .ensureConsumedPast(
            commandStatus1.getCommandSequenceNumber(),
            DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT
        );

    // When:
    final List<KsqlEntity> results = makeMultipleRequest(
        csasSql + "\n"   // <-- commandStatus
            + "CREATE STREAM S2 AS SELECT * FROM test_stream;\n" // <-- commandStatus1
            + "DESCRIBE S;",
        KsqlEntity.class
    );

    // Then:
    verify(commandStore).ensureConsumedPast(
        commandStatus1.getCommandSequenceNumber(), DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT);

    assertThat(results, hasSize(3));
    assertThat(results.get(2), is(instanceOf(SourceDescriptionEntity.class)));
  }

  @Test
  public void shouldNotWaitOnAnyDistributedStatementsBeforeDistributingAnother() throws Exception {
    // When:
    makeMultipleRequest(
        "CREATE STREAM S AS SELECT * FROM test_stream;\n"
            + "CREATE STREAM S2 AS SELECT * FROM test_stream;",
        KsqlEntity.class
    );

    // Then:
    verify(commandStore, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldNotWaitForLastDistributedStatementBeforeExecutingSyncBlackListedStatement()
      throws Exception {
    // Given:
    final ImmutableList<String> blackListed = ImmutableList.of(
        //"LIST TOPICS;" <- mocks don't support required ops,
        "LIST FUNCTIONS;",
        "DESCRIBE FUNCTION LCASE;",
        "LIST PROPERTIES;",
        "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "'='earliest';",
        "UNSET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "';"
    );

    for (final String statement : blackListed) {

      // When:
      makeMultipleRequest(
          "CREATE STREAM " + KsqlIdentifierTestUtil.uniqueIdentifierName()
              + " AS SELECT * FROM test_stream;\n"
              + statement,
          KsqlEntity.class
      );

      // Then:
      verify(commandStore, never()).ensureConsumedPast(anyLong(), any());
    }
  }

  @Test
  public void shouldThrowShutdownIfInterruptedWhileAwaitingPreviousCmdInMultiStatementRequest()
      throws Exception {
    // Given:
    doThrow(new InterruptedException("oh no!"))
        .when(commandStore)
        .ensureConsumedPast(anyLong(), any());

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeMultipleRequest(
            "CREATE STREAM S AS SELECT * FROM test_stream;\n"
                + "DESCRIBE S;",
            KsqlEntity.class
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(SERVICE_UNAVAILABLE.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is("The server is shutting down"))));
    assertThat(e, exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_SERVER_SHUTTING_DOWN))));
  }

  @Test
  public void shouldThrowTimeoutOnTimeoutAwaitingPreviousCmdInMultiStatementRequest()
      throws Exception {
    // Given:
    doThrow(new TimeoutException("oh no!"))
        .when(commandStore)
        .ensureConsumedPast(anyLong(), any());

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeMultipleRequest(
            "CREATE STREAM S AS SELECT * FROM test_stream;\n"
                + "DESCRIBE S;",
            KsqlEntity.class
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(SERVICE_UNAVAILABLE.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(
        containsString("Timed out"))));
    assertThat(e, exceptionErrorMessage(errorMessage(
        containsString("sequence number: " + commandStatus.getCommandSequenceNumber()))));
    assertThat(e, exceptionErrorMessage(errorCode(
        is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT))));
  }

  @Test
  public void shouldFailMultipleStatementsAtomically() {
    // When:
    makeFailingRequest(
        "CREATE STREAM S AS SELECT * FROM test_stream; "
            + "CREATE STREAM S2 AS SELECT * FROM S;"
            + "CREATE STREAM S2 AS SELECT * FROM S;", // <-- duplicate will fail.
        BAD_REQUEST.code()
    );

    // Then:
    verify(commandStore, never()).enqueueCommand(any(), any(), any(Producer.class));
  }

  @Test
  public void shouldDistributeTerminateQuery() {
    // Given:
    final PersistentQueryMetadata queryMetadata = createQuery(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        emptyMap());

    final String terminateSql = "TERMINATE " + queryMetadata.getQueryId() + ";";

    // When:
    final CommandStatusEntity result = makeSingleRequest(terminateSql, CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(terminateSql))),
        any()
    );

    assertThat(result.getStatementText(), is(terminateSql));
  }

  @Test
  public void shouldDistributeTerminateAllQueries() {
    // Given:
    createQuery(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        emptyMap());

    final String terminateSql = "TERMINATE ALL;";

    // When:
    final CommandStatusEntity result = makeSingleRequest(terminateSql, CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(terminateSql))),
        any()
    );

    assertThat(result.getStatementText(), is(terminateSql));
  }

  @Test
  public void shouldThrowOnTerminateUnknownQuery() {
    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest("TERMINATE unknown_query_id;")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is(
        "Unknown queryId: UNKNOWN_QUERY_ID"))));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "TERMINATE unknown_query_id;"))));
  }

  @Test
  public void shouldThrowOnTerminateTerminatedQuery() {
    // Given:
    final String queryId = createQuery(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        emptyMap())
        .getQueryId().toString();

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest(
            "TERMINATE " + queryId + ";"
                + "TERMINATE /*second*/ " + queryId + ";"
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString(
        "Unknown queryId:"))));
    assertThat(e, exceptionStatementErrorMessage(statement(containsString(
        "TERMINATE /*second*/"))));
  }

  @Test
  public void shouldExplainQueryStatement() {
    // Given:
    final String ksqlQueryString = "SELECT * FROM test_stream EMIT CHANGES;";
    final String ksqlString = "EXPLAIN " + ksqlQueryString;

    // When:
    final QueryDescriptionEntity query = makeSingleRequest(
        ksqlString, QueryDescriptionEntity.class);

    // Then:
    validateTransientQueryDescription(ksqlQueryString, emptyMap(), query);
  }

  @Test
  public void shouldExplainCreateAsSelectStatement() {
    // Given:
    final String ksqlQueryString = "CREATE STREAM S3 AS SELECT * FROM test_stream;";
    final String ksqlString = "EXPLAIN " + ksqlQueryString;

    // When:
    final QueryDescriptionEntity query = makeSingleRequest(
        ksqlString, QueryDescriptionEntity.class);

    // Then:
    assertThat("Should not have registered the source",
        metaStore.getSource(SourceName.of("S3")), is(nullValue()));

    validateQueryDescription(ksqlQueryString, emptyMap(), query);
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
        ksqlString, BAD_REQUEST.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(), is("The provided statement does not run a ksql query"));
  }

  @Test
  public void shouldReturn5xxOnSystemError() {
    // Given:
    givenMockEngine();

    when(ksqlEngine.parse(anyString()))
        .thenThrow(new RuntimeException("internal error"));

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        INTERNAL_SERVER_ERROR.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(result.getMessage(), containsString("internal error"));
  }

  @Test
  public void shouldReturn5xxOnStatementSystemError() {
    // Given:
    final String ksqlString = "CREATE STREAM test_explain AS SELECT * FROM test_stream;";
    givenMockEngine();

    reset(sandbox);
    when(sandbox.getMetaStore()).thenReturn(metaStore);
    when(sandbox.prepare(any()))
        .thenAnswer(invocation -> realEngine.createSandbox(serviceContext).prepare(invocation.getArgument(0)));
    when(sandbox.plan(any(), any(ConfiguredStatement.class)))
        .thenThrow(new RuntimeException("internal error"));

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        ksqlString, INTERNAL_SERVER_ERROR.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(result.getMessage(), containsString("internal error"));
  }

  @Test
  public void shouldSetProperty() {
    // Given:
    final String csas = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";

    // When:
    final List<CommandStatusEntity> results = makeMultipleRequest(
        "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'earliest';\n"
            + csas,
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithOverwrittenProperties(
            ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")))),
        any()
    );

    assertThat(results, hasSize(1));
    assertThat(results.get(0).getStatementText(), is(csas));
  }

  @Test
  public void shouldSetPropertyOnlyOnCommandsFollowingTheSetStatement() {
    // Given:
    final String csas = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";

    // When:
    makeMultipleRequest(
        csas +
            "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'earliest';",
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithOverwrittenProperties(emptyMap()))),
        any()
    );
  }

  @Test
  public void shouldFailSetPropertyOnInvalidPropertyName() {
    // When:
    final KsqlErrorMessage response = makeFailingRequest(
        "SET 'ksql.unknown.property' = '1';",
        BAD_REQUEST.code());

    // Then:
    assertThat(response, instanceOf(KsqlStatementErrorMessage.class));
    assertThat(response.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(response.getMessage(), containsString("Unknown property"));
  }

  @Test
  public void shouldFailSetPropertyOnInvalidPropertyValue() {
    // When:
    final KsqlErrorMessage response = makeFailingRequest(
        "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'invalid value';",
        BAD_REQUEST.code());

    // Then:
    assertThat(response, instanceOf(KsqlStatementErrorMessage.class));
    assertThat(response.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(response.getMessage(),
        containsString("Invalid value invalid value for configuration auto.offset.reset: "
            + "String must be one of: latest, earliest, none"));
  }

  @Test
  public void shouldUnsetProperty() {
    // Given:
    final String csas = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";
    final Map<String, Object> localOverrides = ImmutableMap.of(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
    );

    // When:
    final CommandStatusEntity result = makeSingleRequest(
        new KsqlRequest("UNSET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "';\n" + csas,
            localOverrides,
            emptyMap(),
            null),
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithOverwrittenProperties(emptyMap()))),
        any(Producer.class)
    );

    assertThat(result.getStatementText(), is(csas));
  }

  @Test
  public void shouldFailUnsetPropertyOnInvalidPropertyName() {
    // When:
    final KsqlErrorMessage response = makeFailingRequest(
        "UNSET 'ksql.unknown.property';",
        BAD_REQUEST.code());

    // Then:
    assertThat(response, instanceOf(KsqlStatementErrorMessage.class));
    assertThat(response.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(response.getMessage(), containsString("Unknown property"));
  }

  @Test
  public void shouldScopeSetPropertyToSingleRequest() {
    // given:
    final String csas = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";

    makeMultipleRequest(
        "SET '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "' = 'earliest';", KsqlEntity.class);

    // When:
    makeSingleRequest(csas, KsqlEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithOverwrittenProperties(emptyMap()))),
        any()
    );
  }

  @Test
  public void shouldFailIfReachedActivePersistentQueriesLimit() {
    // Given:
    givenKsqlConfigWith(
        ImmutableMap.of(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 3));

    givenMockEngine();

    // mock 3 queries already running + 1 new query to execute
    givenPersistentQueryCount(4);

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM new_stream AS SELECT * FROM test_stream;", BAD_REQUEST.code());

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
        ImmutableMap.of(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 1));

    final String ksqlString = "CREATE STREAM new_stream AS SELECT * FROM test_stream;"
        + "CREATE STREAM another_stream AS SELECT * FROM test_stream;";
    givenMockEngine();

    // mock 2 new query to execute
    givenPersistentQueryCount(2);

    // When:
    final KsqlErrorMessage result = makeFailingRequest(
        ksqlString, BAD_REQUEST.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_REQUEST));
    assertThat(result.getMessage(),
        containsString("would cause the number of active, persistent queries "
            + "to exceed the configured limit"));

    verify(commandStore, never()).enqueueCommand(any(), any(), any());
  }

  @Test
  public void shouldListPropertiesWithOverrides() {
    // Given:
    final Map<String, Object> overrides = Collections.singletonMap("auto.offset.reset", "latest");

    // When:
    final PropertiesList props = makeSingleRequest(
        new KsqlRequest("list properties;", overrides, emptyMap(), null),
        PropertiesList.class);

    // Then:
    assertThat(
        props.getProperties(),
        hasItem(new Property("ksql.streams.auto.offset.reset", "KSQL", "latest")));
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
    assertThat(props.getProperties().stream().map(Property::getName).collect(
        Collectors.toList()),
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
        makeFailingRequestWithSequenceNumber("list properties;", 2L, SERVICE_UNAVAILABLE.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT));
    assertThat(result.getMessage(),
        containsString("Timed out while waiting for a previous command to execute"));
    assertThat(result.getMessage(), containsString("command sequence number: 2"));
  }

  @Test
  public void shouldUpdateTheLastRequestTime() {
    // When:
    ksqlResource.handleKsqlStatements(securityContext, VALID_EXECUTABLE_REQUEST);

    // Then:
    verify(activenessRegistrar).updateLastRequestTime();
  }

  @Test
  public void shouldHandleTerminateRequestCorrectly() {
    // When:
    final EndpointResponse response = ksqlResource.terminateCluster(
        securityContext,
        VALID_TERMINATE_REQUEST
    );

    // Then:
    assertThat(response.getStatus(), equalTo(200));
    assertThat(response.getEntity(), instanceOf(KsqlEntityList.class));
    assertThat(((KsqlEntityList) response.getEntity()).size(), equalTo(1));
    assertThat(((KsqlEntityList) response.getEntity()).get(0),
        instanceOf(CommandStatusEntity.class));
    final CommandStatusEntity commandStatusEntity =
        (CommandStatusEntity) ((KsqlEntityList) response.getEntity()).get(0);
    assertThat(commandStatusEntity.getCommandStatus().getStatus(),
        equalTo(CommandStatus.Status.QUEUED));
    verify(transactionalProducer, times(1)).initTransactions();
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT))),
        any()
    );
  }

  @Test
  public void shouldFailIfCannotWriteTerminateCommand() {
    // Given:
    when(commandStore.enqueueCommand(any(), any(), any(Producer.class)))
        .thenThrow(new KsqlException(""));

    // When:
    final EndpointResponse response = ksqlResource.terminateCluster(
        securityContext,
        VALID_TERMINATE_REQUEST
    );

    // Then:
    assertThat(response.getStatus(), equalTo(500));
    assertThat(response.getEntity().toString(),
        CoreMatchers
            .startsWith("Could not write the statement 'TERMINATE CLUSTER;' into the command "));
  }

  @Test
  public void shouldFailTerminateOnInvalidDeleteTopicPattern() {
    // Given:
    final ClusterTerminateRequest request = new ClusterTerminateRequest(
        ImmutableList.of("[Invalid Regex"));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> ksqlResource.terminateCluster(securityContext, request)
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is(
        "Invalid pattern: [Invalid Regex"))));
  }

  @Test
  public void shouldNeverEnqueueIfErrorIsThrown() {
    // Given:
    givenMockEngine();
    when(ksqlEngine.parse(anyString())).thenThrow(new KsqlException("Fail"));

    // When:
    makeFailingRequest(
        "LIST TOPICS;",
        BAD_REQUEST.code());

    // Then:
    verify(commandStore, never()).enqueueCommand(any(), any(), any(Producer.class));
  }

  @Test
  public void shouldFailIfCreateExistingSourceStream() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "SOURCE", "topic1", SINGLE_FIELD_SCHEMA);
    givenKafkaTopicExists("topic2");
    final String createSql =
        "CREATE STREAM SOURCE (val int) WITH (kafka_topic='topic2', value_format='json');";

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeSingleRequest(createSql, CommandStatusEntity.class)
);

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString(
        "Cannot add stream 'SOURCE': A stream with the same name already exists"))));
  }

  @Test
  public void shouldFailIfCreateExistingSourceTable() {
    // Given:
    givenSource(DataSourceType.KTABLE, "SOURCE", "topic1", SINGLE_FIELD_SCHEMA);
    givenKafkaTopicExists("topic2");
    final String createSql =
        "CREATE TABLE SOURCE (id int primary key, val int) WITH (kafka_topic='topic2', value_format='json');";

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeSingleRequest(createSql, CommandStatusEntity.class)
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString(
        "Cannot add table 'SOURCE': A table with the same name already exists"))));
  }

  @Test
  public void shouldFailIfCreateAsSelectExistingSourceStream() {
    // Given:
    givenSource(DataSourceType.KSTREAM, "SOURCE", "topic1", SINGLE_FIELD_SCHEMA);
    givenSource(DataSourceType.KTABLE, "SINK", "topic2", SINGLE_FIELD_SCHEMA);
    final String createSql = "CREATE STREAM SINK AS SELECT * FROM SOURCE;";

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeSingleRequest(createSql, CommandStatusEntity.class)
);

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString(
        "Cannot add stream 'SINK': A table with the same name already exists"))));
  }

  @Test
  public void shouldFailIfCreateAsSelectExistingSourceTable() {
    // Given:
    givenSource(DataSourceType.KTABLE, "SOURCE", "topic1", SINGLE_FIELD_SCHEMA);
    givenSource(DataSourceType.KSTREAM, "SINK", "topic2", SINGLE_FIELD_SCHEMA);
    final String createSql =
        "CREATE TABLE SINK AS SELECT * FROM SOURCE;";

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeSingleRequest(createSql, CommandStatusEntity.class)
);

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString(
        "Cannot add table 'SINK': A stream with the same name already exists"))));
  }

  @Test
  public void shouldThrowServerErrorOnFailedToDistribute() {
    // Given:
    when(commandStore.enqueueCommand(any(), any(), any(Producer.class)))
        .thenThrow(new KsqlException("blah"));
    final String statement = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest(statement)
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(INTERNAL_SERVER_ERROR.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is(
        "Could not write the statement '" + statement
            + "' into the command topic." + System.lineSeparator() + "Caused by: blah"))));
  }

  private Answer<?> executeAgainstEngine(final String sql) {
    return invocation -> {
      KsqlEngineTestUtil.execute(serviceContext, ksqlEngine, sql, ksqlConfig, emptyMap());
      return null;
    };
  }

  @SuppressWarnings("SameParameterValue")
  private SourceInfo.Table sourceTable(final String name) {
    final KsqlTable<?> table = (KsqlTable) ksqlEngine.getMetaStore().getSource(SourceName.of(name));
    return new SourceInfo.Table(
        table.getName().toString(FormatOptions.noEscape()),
        table.getKsqlTopic().getKafkaTopicName(),
        table.getKsqlTopic().getValueFormat().getFormat().name(),
        table.getKsqlTopic().getKeyFormat().isWindowed()
    );
  }

  @SuppressWarnings("SameParameterValue")
  private SourceInfo.Stream sourceStream(final String name) {
    final KsqlStream<?> stream = (KsqlStream) ksqlEngine.getMetaStore().getSource(
        SourceName.of(name));
    return new SourceInfo.Stream(
        stream.getName().toString(FormatOptions.noEscape()),
        stream.getKsqlTopic().getKafkaTopicName(),
        stream.getKsqlTopic().getValueFormat().getFormat().name()
    );
  }

  private void givenMockEngine() {
    ksqlEngine = mock(KsqlEngine.class);
    when(ksqlEngine.parse(any()))
        .thenAnswer(invocation -> realEngine.parse(invocation.getArgument(0)));
    when(ksqlEngine.prepare(any()))
        .thenAnswer(invocation -> realEngine.prepare(invocation.getArgument(0)));
    when(sandbox.prepare(any()))
        .thenAnswer(invocation -> realEngine.createSandbox(serviceContext).prepare(invocation.getArgument(0)));
    when(sandbox.plan(any(), any())).thenAnswer(
        i -> KsqlPlan.ddlPlanCurrent(
            ((ConfiguredStatement<?>) i.getArgument(1)).getStatementText(),
            new DropSourceCommand(SourceName.of("bob"))
        )
    );
    when(ksqlEngine.createSandbox(any())).thenReturn(sandbox);
    when(ksqlEngine.getMetaStore()).thenReturn(metaStore);
    when(topicInjectorFactory.apply(ksqlEngine)).thenReturn(topicInjector);
    setUpKsqlResource();
  }

  private List<PersistentQueryMetadata> createQueries(
      final String sql,
      final Map<String, Object> overriddenProperties) {
    return KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        sql,
        ksqlConfig,
        overriddenProperties
    ).stream()
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
            ImmutableSet.of(md.getSinkName().toString(FormatOptions.noEscape())),
            ImmutableSet.of(md.getResultTopic().getKafkaTopicName()),
            md.getQueryId(),
            QueryStatusCount.fromStreamsStateCounts(
                Collections.singletonMap(md.getState(), 1)), KsqlConstants.KsqlQueryType.PERSISTENT)
    ).collect(Collectors.toList());
  }

  private KsqlErrorMessage makeFailingRequest(final String ksql, final int errorCode) {
    return makeFailingRequestWithSequenceNumber(ksql, null, errorCode);
  }

  private KsqlErrorMessage makeFailingRequestWithSequenceNumber(
      final String ksql,
      final Long seqNum,
      final int errorCode) {
    return makeFailingRequest(new KsqlRequest(ksql, emptyMap(), emptyMap(), seqNum), errorCode);
  }

  private KsqlErrorMessage makeFailingRequest(final KsqlRequest ksqlRequest, final int errorCode) {
    try {
      final EndpointResponse response = ksqlResource
          .handleKsqlStatements(securityContext, ksqlRequest);
      assertThat(response.getStatus(), is(errorCode));
      assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
      return (KsqlErrorMessage) response.getEntity();
    } catch (final KsqlRestException e) {
      return (KsqlErrorMessage) e.getResponse().getEntity();
    }
  }

  private void makeRequest(final String sql) {
    makeMultipleRequest(sql, KsqlEntity.class);
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
        new KsqlRequest(sql, emptyMap(), emptyMap(), seqNum), expectedEntityType);
  }

  private <T extends KsqlEntity> T makeSingleRequest(
      final KsqlRequest ksqlRequest,
      final Class<T> expectedEntityType) {

    final List<T> entities = makeMultipleRequest(ksqlRequest, expectedEntityType);
    assertThat(entities, hasSize(1));
    return entities.get(0);
  }

  private <T extends KsqlEntity> List<T> makeMultipleRequest(
      final String sql,
      final Class<T> expectedEntityType
  ) {
    return makeMultipleRequest(sql, emptyMap(), expectedEntityType);
  }

  private <T extends KsqlEntity> List<T> makeMultipleRequest(
      final String sql,
      final Map<String, ?> props,
      final Class<T> expectedEntityType
  ) {
    final KsqlRequest request = new KsqlRequest(sql, props, emptyMap(), null);
    return makeMultipleRequest(request, expectedEntityType);
  }

  private <T extends KsqlEntity> List<T> makeMultipleRequest(
      final KsqlRequest ksqlRequest,
      final Class<T> expectedEntityType) {

    final EndpointResponse response = ksqlResource
        .handleKsqlStatements(securityContext, ksqlRequest);
    if (response.getStatus() != OK.code()) {
      throw new KsqlRestException(response);
    }

    final Object entity = response.getEntity();
    assertThat(entity, instanceOf(KsqlEntityList.class));
    final KsqlEntityList entityList = (KsqlEntityList) entity;
    entityList.forEach(e -> assertThat(e, instanceOf(expectedEntityType)));
    return entityList.stream()
        .map(expectedEntityType::cast)
        .collect(Collectors.toList());
  }

  private void validateTransientQueryDescription(
      final String ksqlQueryString,
      final Map<String, Object> overriddenProperties,
      final KsqlEntity entity) {
    final TransientQueryMetadata queryMetadata = KsqlEngineTestUtil.executeQuery(
        serviceContext,
        ksqlEngine,
        ksqlQueryString,
        ksqlConfig,
        overriddenProperties
    );
    validateQueryDescription(queryMetadata, overriddenProperties, entity);
  }

  @SuppressWarnings("SameParameterValue")
  private void validateQueryDescription(
      final String ksqlQueryString,
      final Map<String, Object> overriddenProperties,
      final KsqlEntity entity) {
    final QueryMetadata queryMetadata = KsqlEngineTestUtil
        .execute(
            serviceContext,
            ksqlEngine,
            ksqlQueryString,
            ksqlConfig,
            overriddenProperties)
        .get(0);

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
        EntityUtil.buildSourceSchemaEntity(queryMetadata.getLogicalSchema())));
    assertThat(queryDescription.getOverriddenProperties(), is(overriddenProperties));
  }

  private void setUpKsqlResource() {
    ksqlResource = new KsqlResource(
        ksqlEngine,
        commandRunner,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT,
        activenessRegistrar,
        (ec, sc) -> InjectorChain.of(
            schemaInjectorFactory.apply(sc),
            topicInjectorFactory.apply(ec),
            new TopicDeleteInjector(ec, sc)),
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator
    );

    ksqlResource.configure(ksqlConfig);
  }

  @Test
  public void shouldThrowOnDenyListValidatorWhenTerminateCluster() {
    final Map<String, Object> terminateStreamProperties =
        ImmutableMap.of(DELETE_TOPIC_LIST_PROP, Collections.singletonList("Foo"));

    // Given:
    doThrow(new KsqlException("deny override")).when(denyListPropertyValidator).validateAll(
        terminateStreamProperties
    );

    // When:
    final EndpointResponse response = ksqlResource.terminateCluster(
        securityContext,
        VALID_TERMINATE_REQUEST
    );

    // Then:
    verify(denyListPropertyValidator).validateAll(terminateStreamProperties);
    assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.code()));
    assertThat(response.getEntity(), instanceOf(KsqlStatementErrorMessage.class));
    assertThat(((KsqlStatementErrorMessage) response.getEntity()).getMessage(),
        containsString("deny override"));
  }

  @Test
  public void shouldThrowOnDenyListValidatorWhenHandleKsqlStatement() {
    // Given:
    ksqlResource = new KsqlResource(
        ksqlEngine,
        commandRunner,
        DISTRIBUTED_COMMAND_RESPONSE_TIMEOUT,
        activenessRegistrar,
        (ec, sc) -> InjectorChain.of(
            schemaInjectorFactory.apply(sc),
            topicInjectorFactory.apply(ec),
            new TopicDeleteInjector(ec, sc)),
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator
    );
    final Map<String, Object> props = new HashMap<>(ksqlRestConfig.getKsqlConfigProperties());
    props.put(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG);
    ksqlResource.configure(new KsqlConfig(props));
    final Map<String, Object> overrides =
        ImmutableMap.of(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    doThrow(new KsqlException("deny override")).when(denyListPropertyValidator)
        .validateAll(overrides);

    // When:
    final EndpointResponse response = ksqlResource.handleKsqlStatements(
        securityContext,
        new KsqlRequest(
            "query",
            overrides,  // stream properties
            emptyMap(),
            null
        )
    );

    // Then:
    verify(denyListPropertyValidator).validateAll(overrides);
    assertThat(response.getStatus(), CoreMatchers.is(BAD_REQUEST.code()));
    assertThat(((KsqlErrorMessage) response.getEntity()).getMessage(), is("deny override"));
  }

  private void givenKsqlConfigWith(final Map<String, Object> additionalConfig) {
    final Map<String, Object> config = ksqlRestConfig.getKsqlConfigProperties();
    config.putAll(additionalConfig);
    ksqlConfig = new KsqlConfig(config);

    setUpKsqlResource();
  }

  private void addTestTopicAndSources() {
    final LogicalSchema schema1 = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("S1_F1"), SqlTypes.BOOLEAN)
        .build();

    givenSource(
        DataSourceType.KTABLE,
        "TEST_TABLE", "KAFKA_TOPIC_1", schema1);

    final LogicalSchema schema2 = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("S2_F1"), SqlTypes.STRING)
        .build();

    givenSource(
        DataSourceType.KSTREAM,
        "TEST_STREAM", "KAFKA_TOPIC_2", schema2);
    givenKafkaTopicExists("orders-topic");
  }

  private void givenSource(
      final DataSourceType type,
      final String sourceName,
      final String topicName,
      final LogicalSchema schema
  ) {
    final KsqlTopic ksqlTopic = new KsqlTopic(
        topicName,
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()))
    );

    givenKafkaTopicExists(topicName);
    if (type == DataSourceType.KSTREAM) {
      metaStore.putSource(
          new KsqlStream<>(
              "statementText",
              SourceName.of(sourceName),
              schema,
              SerdeOptions.of(),
              Optional.empty(),
              false,
              ksqlTopic
          ), false);
    }
    if (type == DataSourceType.KTABLE) {
      metaStore.putSource(
          new KsqlTable<>(
              "statementText",
              SourceName.of(sourceName),
              schema,
              SerdeOptions.of(),
              Optional.empty(),
              false,
              ksqlTopic
          ), false);
    }
  }

  private static Properties getDefaultKsqlConfig() {
    final Map<String, Object> configMap = new HashMap<>(KsqlConfigTestUtil.baseTestConfig());
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("ksql.command.topic.suffix", "commands");
    configMap.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088");
    configMap.put(StreamsConfig.APPLICATION_SERVER_CONFIG, APPLICATION_SERVER);

    final Properties properties = new Properties();
    properties.putAll(configMap);

    return properties;
  }

  private static void registerSchema(final SchemaRegistryClient schemaRegistryClient)
      throws IOException, RestClientException {
    final String ordersAvroSchemaStr = "{"
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
    final org.apache.avro.Schema avroSchema = parser.parse(ordersAvroSchemaStr);
    schemaRegistryClient.register("orders-topic" + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX,
        new AvroSchema(avroSchema));
  }

  private static Matcher<Command> commandWithStatement(final String statement) {
    return new TypeSafeMatcher<Command>() {
      @Override
      protected boolean matchesSafely(final Command command) {
        return command.getStatement().equals(statement);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(statement);
      }
    };
  }

  private static Matcher<Command> commandWithOverwrittenProperties(
      final Map<String, Object> properties
  ) {
    return new TypeSafeMatcher<Command>() {
      @Override
      protected boolean matchesSafely(final Command command) {
        return command.getOverwriteProperties().equals(properties);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(properties.toString());
      }
    };
  }

  @SuppressWarnings("SameParameterValue")
  private void givenAvroSchemaNotEvolveable(final String topicName) {
    final org.apache.avro.Schema schema = org.apache.avro.Schema.create(Type.INT);

    try {
      schemaRegistryClient.register(
          topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, new AvroSchema(schema));
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  private void givenKafkaTopicExists(final String name) {
    kafkaTopicClient.preconditionTopicExists(name, 1, (short) 1, emptyMap());
  }

  private void givenPersistentQueryCount(final int value) {
    final List<PersistentQueryMetadata> queries = mock(List.class);
    when(queries.size()).thenReturn(value);
    when(sandbox.getPersistentQueries()).thenReturn(queries);
  }

  @Sandbox
  private interface SandboxEngine extends KsqlExecutionContext {
  }
}
