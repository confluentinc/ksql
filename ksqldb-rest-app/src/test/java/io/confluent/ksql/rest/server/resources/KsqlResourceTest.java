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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
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
import io.confluent.ksql.config.SessionConfig;
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
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.query.id.SequentialQueryIdGenerator;
import io.confluent.ksql.rest.DefaultErrorMessages;
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
import io.confluent.ksql.serde.SerdeFeatures;
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
import io.confluent.ksql.util.QueryMask;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.Sandbox;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;
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
      new TableElement(ColumnName.of("f0"), new io.confluent.ksql.execution.expression.tree.Type(SqlTypes.STRING))
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
              "KEY_FORMAT", new StringLiteral("kafka"),
              "VALUE_FORMAT", new StringLiteral("avro")
          )),
          false));

  private static final Properties DEFAULT_KSQL_CONFIG = getDefaultKsqlConfig();

  private static final ConfiguredStatement<CreateStream> CFG_0_WITH_SCHEMA =
      ConfiguredStatement.of(
        STMT_0_WITH_SCHEMA,
        SessionConfig.of(new KsqlConfig(DEFAULT_KSQL_CONFIG), ImmutableMap.of())
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
              "KEY_FORMAT", new StringLiteral("kafka"),
              "VALUE_FORMAT", new StringLiteral("avro")
          )),
          false));
  private static final ConfiguredStatement<CreateStream> CFG_1_WITH_SCHEMA = ConfiguredStatement
      .of(STMT_1_WITH_SCHEMA,
          SessionConfig.of(new KsqlConfig(DEFAULT_KSQL_CONFIG), ImmutableMap.of())
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
  @Mock
  private Supplier<String> commandRunnerWarning;
  @Mock
  private Optional<PersistentQueryMetadata> persistentQuery;

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
    VALID_EXECUTABLE_REQUEST.setMaskedKsql(QueryMask.getMaskedStatement(VALID_EXECUTABLE_REQUEST.getUnmaskedKsql()));

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
    registerValueSchema(schemaRegistryClient);
    ksqlRestConfig = new KsqlRestConfig(DEFAULT_KSQL_CONFIG);
    ksqlConfig = new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties());
    final KsqlExecutionContext.ExecuteResult result = mock(KsqlExecutionContext.ExecuteResult.class);
    when(sandbox.execute(any(), any(ConfiguredKsqlPlan.class))).thenReturn(result);
    when(result.getQuery()).thenReturn(Optional.empty());

    MutableFunctionRegistry fnRegistry = new InternalFunctionRegistry();
    final Metrics metrics = new Metrics();
    UserFunctionLoader.newInstance(ksqlConfig, fnRegistry, ".",
        metrics
    ).load();
    metaStore = new MetaStoreImpl(fnRegistry);
    final MetricCollectors metricCollectors = new MetricCollectors(metrics);
    realEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore,
        (engine) -> new KsqlEngineMetrics(
            "",
            engine,
            Collections.emptyMap(),
            Optional.empty(),
            metricCollectors
        ),
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        metricCollectors
    );

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    when(commandRunner.getCommandQueue()).thenReturn(commandStore);
    when(commandRunnerWarning.get()).thenReturn("");
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
        denyListPropertyValidator,
        commandRunnerWarning
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
        denyListPropertyValidator,
        commandRunnerWarning
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
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            ),
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("new_stream")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("new_topic")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            )
        )
    );
  }

  @Test
  public void shouldDescribeStreams() {
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
        "DESCRIBE STREAMS;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_STREAM")),
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_2")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            ),
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("new_stream")),
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("new_topic")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            )
        )
    );
  }

  @Test
  public void shouldDescribeStreamsExtended() {
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
        "DESCRIBE STREAMS EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_STREAM")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_2")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            ),
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("new_stream")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("new_topic")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            )
        )
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
        schema, ImmutableSet.of(SourceName.of("TEST_TABLE")));

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "SHOW TABLES EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_TABLE")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_1")),
                Collections.emptyList(),
                ImmutableList.of("new_table"),
                new MetricCollectors()
            ),
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("new_table")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("new_topic")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            )
        )
    );
  }

  @Test
  public void shouldDescribeTables() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("FIELD1"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("FIELD2"), SqlTypes.STRING)
        .build();

    givenSource(
        DataSourceType.KTABLE, "new_table", "new_topic",
        schema, ImmutableSet.of(SourceName.of("TEST_TABLE")));

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "DESCRIBE TABLES;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_TABLE")),
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_1")),
                Collections.emptyList(),
                ImmutableList.of("new_table"),
                new MetricCollectors()
            ),
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("new_table")),
                false,
                Collections.emptyList(),
                Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("new_topic")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            )
        )
    );
  }

  @Test
  public void shouldDescribeTablesExtended() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("FIELD1"), SqlTypes.BOOLEAN)
        .valueColumn(ColumnName.of("FIELD2"), SqlTypes.STRING)
        .build();

    givenSource(
        DataSourceType.KTABLE, "new_table", "new_topic",
        schema, ImmutableSet.of(SourceName.of("TEST_TABLE")));

    // When:
    final SourceDescriptionList descriptionList = makeSingleRequest(
        "DESCRIBE TABLES EXTENDED;", SourceDescriptionList.class);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("TEST_TABLE")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("KAFKA_TOPIC_1")),
                Collections.emptyList(),
                ImmutableList.of("new_table"),
                new MetricCollectors()
            ),
            SourceDescriptionFactory.create(
                ksqlEngine.getMetaStore().getSource(SourceName.of("new_table")),
                true, Collections.emptyList(), Collections.emptyList(),
                Optional.of(kafkaTopicClient.describeTopic("new_topic")),
                Collections.emptyList(),
                Collections.emptyList(),
                new MetricCollectors()
            )
        )
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
  public void shouldHaveKsqlWarningIfCommandRunnerDegraded() {
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
    final SourceDescriptionList descriptionList1 = makeSingleRequest(
        "SHOW STREAMS EXTENDED;", SourceDescriptionList.class);
    when(commandRunnerWarning.get()).thenReturn(DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE);
    final SourceDescriptionList descriptionList2 = makeSingleRequest(
        "SHOW STREAMS EXTENDED;", SourceDescriptionList.class);

    assertThat(descriptionList1.getWarnings().size(), is(0));
    assertThat(descriptionList2.getWarnings().size(), is(1));
    assertThat(descriptionList2.getWarnings().get(0).getMessage(), is(DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
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
        Collections.emptyList(),
        Collections.emptyList(),
        new MetricCollectors()
    );

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
        "CREATE STREAM S (foo INT) WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON');",
        BAD_REQUEST.code());

    // Then:
    assertThat(result, is(instanceOf(KsqlStatementErrorMessage.class)));
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        containsString("Missing required property \"KAFKA_TOPIC\" which has no default value."));
    assertThat(((KsqlStatementErrorMessage) result).getStatementText(),
        is("CREATE STREAM S (foo INT) WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldReturnCustomErrorOnRawException() {
    // Given:
    final String errorMsg = "some error";
    final Exception e = new KsqlTopicAuthorizationException(
        AclOperation.DELETE,
        Collections.singleton("topic"));
    when(errorsHandler.generateResponse(eq(e), any())).thenReturn(EndpointResponse.create()
        .status(FORBIDDEN.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, errorMsg))
        .build());
    doThrow(e).when(authorizationValidator).checkAuthorization(any(), any(), any());

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
  public void shouldReturnCustomErrorOnKsqlException() {
    // Given:
    final String errorMsg = "some error";
    final Exception e = new KsqlException(new KsqlTopicAuthorizationException(
        AclOperation.DELETE,
        Collections.singleton("topic")));
    when(errorsHandler.generateResponse(eq(e), any())).thenReturn(EndpointResponse.create()
        .status(FORBIDDEN.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, errorMsg))
        .build());
    doThrow(e).when(authorizationValidator).checkAuthorization(any(), any(), any());

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
  public void shouldReturnCustomErrorOnKsqlStatementException() {
    // Given:
    final String errorMsg = "some error";
    final Exception e = new KsqlStatementException("foo", "sql",
        new KsqlTopicAuthorizationException(AclOperation.DELETE, Collections.singleton("topic")));
    when(errorsHandler.generateResponse(eq(e), any())).thenReturn(EndpointResponse.create()
        .status(FORBIDDEN.code())
        .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, errorMsg))
        .build());
    doThrow(e).when(authorizationValidator).checkAuthorization(any(), any(), any());

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
        () -> makeRequest("CREATE STREAM S (foo INT) WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', KAFKA_TOPIC='unknown');")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is("Kafka topic does not exist: unknown"))));
  }

  @Test
  public void shouldDistributeAvroCreateStatementWithColumns() {
    // When:
    makeSingleRequest(
        "CREATE STREAM S (foo INT) WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');",
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        any(),
        argThat(is(commandWithStatement(
            "CREATE STREAM S (foo INT) WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');"))),
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
        ksqlEngine.prepare(ksqlEngine.parse(sqlWithTopic).get(0), Collections.emptyMap());
    final ConfiguredStatement<?> configuredStatement =
        ConfiguredStatement.of(statementWithTopic, SessionConfig.of(ksqlConfig, ImmutableMap.of())
        );

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
        ksqlEngine.prepare(ksqlEngine.parse(sqlWithTopic).get(0), Collections.emptyMap());
    final ConfiguredStatement<?> configured =
        ConfiguredStatement.of(statementWithTopic, SessionConfig.of(ksqlConfig, ImmutableMap.of())
        );

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
  public void shouldSupportVariableSubstitution() {
    // Given:
    final String csasRaw = "CREATE STREAM ${streamName} AS SELECT * FROM ${fromStream};";
    final String csasSubstituted = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";


    // When:
    final List<CommandStatusEntity> results = makeMultipleRequest(
        "DEFINE streamName = '" + streamName + "';\n"
            + "DEFINE fromStream = 'test_stream';\n"
            + csasRaw,
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        argThat(is(commandIdWithString("stream/`" + streamName + "`/create"))),
        argThat(is(commandWithStatement(csasSubstituted))),
        any()
    );

    assertThat(results, hasSize(1));
    assertThat(results.get(0).getStatementText(), is(csasSubstituted));
  }

  @Test
  public void shouldSupportVariableSubstitutionWithVariablesInRequest() {
    // Given:
    final String csasRaw = "CREATE STREAM ${streamName} AS SELECT * FROM ${fromStream};";
    final String csasSubstituted = "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;";


    // When:
    final List<CommandStatusEntity> results = makeMultipleRequest(
        new KsqlRequest(
            csasRaw,
            emptyMap(),
            emptyMap(),
            ImmutableMap.of("streamName", streamName, "fromStream", "test_stream"),
            null),
        CommandStatusEntity.class);

    // Then:
    verify(commandStore).enqueueCommand(
        argThat(is(commandIdWithString("stream/`" + streamName + "`/create"))),
        argThat(is(commandWithStatement(csasSubstituted))),
        any()
    );

    assertThat(results, hasSize(1));
    assertThat(results.get(0).getStatementText(), is(csasSubstituted));
  }

  @Test
  public void shouldSupportSchemaInference() {
    // Given:
    givenMockEngine();

    final String sql = "CREATE STREAM NO_SCHEMA WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');";

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
        argThat(is(commandWithStatement(CFG_1_WITH_SCHEMA.getUnMaskedStatementText()))),
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
        "CREATE STREAM S WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');",
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
        () -> makeRequest("CREATE STREAM S WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');")
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
        () -> makeRequest("CREATE STREAM S WITH(KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', KAFKA_TOPIC='orders-topic');")
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_BAD_STATEMENT))));
    assertThat(e, exceptionErrorMessage(errorMessage(is("The statement does not define any columns."))));
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
  public void shouldThrowOnInsertBadQuery() {
    // When:
    final String query = "--this is a comment. \n"
        + "INSERT INTO foo (KEY_COL, COL_A) VALUES"
        + "(\"key\", 0.125, 1);";
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> makeRequest(query)
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionStatementErrorMessage(statement(is(
        "INSERT INTO `FOO` (`KEY_COL`, `COL_A`) VALUES ('[value]', '[value]', '[value]');"))));
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
    when(sandbox.prepare(any(), any()))
        .thenAnswer(invocation ->
            realEngine.createSandbox(serviceContext)
                .prepare(invocation.getArgument(0), Collections.emptyMap()));
    when(sandbox.plan(any(), any(ConfiguredStatement.class)))
        .thenThrow(new RuntimeException("internal error"));
    when(sandbox.getKsqlConfig()).thenReturn(ksqlConfig);

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
  public void shouldRejectQueryButAcceptNonQueryWhenKsqlRestartsWithLowerQueryLimit() {
    // Given 6 queries already running:
    givenPersistentQueryCount(6);

    // When we restart ksql with a lower persistent query count
    givenKsqlConfigWith(
        ImmutableMap.of(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG, 3));
    givenMockEngine();

    // When/Then:
    makeSingleRequest("SHOW STREAMS;", StreamsList.class);

    // No further queries can be made
    final KsqlErrorMessage result = makeFailingRequest(
        "CREATE STREAM " + streamName + " AS SELECT * FROM test_stream;", BAD_REQUEST.code());
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
        .put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + File.separator + "kafka-streams")
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
  public void shouldReturnErrorMessageIfStackOverflowErrorWhenParsingStatement() {
    givenKafkaTopicExists("topic");
    final String firstStatement = "CREATE STREAM FIRST (ITEM_ID VARCHAR, ITEM BIGINT) WITH (KAFKA_TOPIC='topic', KEY_FORMAT='KAFKA', PARTITIONS=2, VALUE_FORMAT='JSON');";
    final String secondStatement = "CREATE STREAM SECOND AS SELECT ITM.ITEM_ID FROM FIRST ITM WHERE ((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((ITM.ITEM_ID = '13003') OR (ITM.ITEM_ID = '21817')) OR (ITM.ITEM_ID = '31119')) OR (ITM.ITEM_ID = '31816')) OR (ITM.ITEM_ID = '41025')) OR (ITM.ITEM_ID = '45750')) OR (ITM.ITEM_ID = '46000')) OR (ITM.ITEM_ID = '50305')) OR (ITM.ITEM_ID = '50307')) OR (ITM.ITEM_ID = '50403')) OR (ITM.ITEM_ID = '50404')) OR (ITM.ITEM_ID = '53017')) OR (ITM.ITEM_ID = '53317')) OR (ITM.ITEM_ID = '57285')) OR (ITM.ITEM_ID = '58589')) OR (ITM.ITEM_ID = '58590')) OR (ITM.ITEM_ID = '58865')) OR (ITM.ITEM_ID = '60423')) OR (ITM.ITEM_ID = '61155')) OR (ITM.ITEM_ID = '61158')) OR (ITM.ITEM_ID = '61305')) OR (ITM.ITEM_ID = '61307')) OR (ITM.ITEM_ID = '62337')) OR (ITM.ITEM_ID = '62357')) OR (ITM.ITEM_ID = '62519')) OR (ITM.ITEM_ID = '62564')) OR (ITM.ITEM_ID = '63079')) OR (ITM.ITEM_ID = '63086')) OR (ITM.ITEM_ID = '63128')) OR (ITM.ITEM_ID = '63130')) OR (ITM.ITEM_ID = '63131')) OR (ITM.ITEM_ID = '63132')) OR (ITM.ITEM_ID = '63163')) OR (ITM.ITEM_ID = '63164')) OR (ITM.ITEM_ID = '63230')) OR (ITM.ITEM_ID = '63231')) OR (ITM.ITEM_ID = '63233')) OR (ITM.ITEM_ID = '63234')) OR (ITM.ITEM_ID = '63235')) OR (ITM.ITEM_ID = '63237')) OR (ITM.ITEM_ID = '63238')) OR (ITM.ITEM_ID = '63247')) OR (ITM.ITEM_ID = '63258')) OR (ITM.ITEM_ID = '63267')) OR (ITM.ITEM_ID = '63280')) OR (ITM.ITEM_ID = '63283')) OR (ITM.ITEM_ID = '63328')) OR (ITM.ITEM_ID = '0128562')) OR (ITM.ITEM_ID = '0129495')) OR (ITM.ITEM_ID = '0135810')) OR (ITM.ITEM_ID = '0135823')) OR (ITM.ITEM_ID = '0136162')) OR (ITM.ITEM_ID = '0136184')) OR (ITM.ITEM_ID = '0136213')) OR (ITM.ITEM_ID = '0139833')) OR (ITM.ITEM_ID = '0150225')) OR (ITM.ITEM_ID = '0150942')) OR (ITM.ITEM_ID = '0151017')) OR (ITM.ITEM_ID = '0158901')) OR (ITM.ITEM_ID = '0159638')) OR (ITM.ITEM_ID = '0159640')) OR (ITM.ITEM_ID = '0160298')) OR (ITM.ITEM_ID = '0175925')) OR (ITM.ITEM_ID = '0200067')) OR (ITM.ITEM_ID = '0200092')) OR (ITM.ITEM_ID = '0200213')) OR (ITM.ITEM_ID = '0200216')) OR (ITM.ITEM_ID = '0200218')) OR (ITM.ITEM_ID = '0200424')) OR (ITM.ITEM_ID = '0200660')) OR (ITM.ITEM_ID = '0200661')) OR (ITM.ITEM_ID = '0201625')) OR (ITM.ITEM_ID = '0202287')) OR (ITM.ITEM_ID = '0203486')) OR (ITM.ITEM_ID = '0204792')) OR (ITM.ITEM_ID = '0205164')) OR (ITM.ITEM_ID = '0205316')) OR (ITM.ITEM_ID = '0205478')) OR (ITM.ITEM_ID = '0205479')) OR (ITM.ITEM_ID = '0205481')) OR (ITM.ITEM_ID = '0206884')) OR (ITM.ITEM_ID = '0209250')) OR (ITM.ITEM_ID = '0209257')) OR (ITM.ITEM_ID = '0210025')) OR (ITM.ITEM_ID = '0210563')) OR (ITM.ITEM_ID = '0211480')) OR (ITM.ITEM_ID = '0211808')) OR (ITM.ITEM_ID = '0211828')) OR (ITM.ITEM_ID = '0211842')) OR (ITM.ITEM_ID = '0211848')) OR (ITM.ITEM_ID = '0211969')) OR (ITM.ITEM_ID = '0212719')) OR (ITM.ITEM_ID = '0212723')) OR (ITM.ITEM_ID = '0215006')) OR (ITM.ITEM_ID = '0215007')) OR (ITM.ITEM_ID = '0215009')) OR (ITM.ITEM_ID = '0218173')) OR (ITM.ITEM_ID = '0222954')) OR (ITM.ITEM_ID = '0222957')) OR (ITM.ITEM_ID = '0222959')) OR (ITM.ITEM_ID = '0222960')) OR (ITM.ITEM_ID = '0222961')) OR (ITM.ITEM_ID = '0222965')) OR (ITM.ITEM_ID = '0224181')) OR (ITM.ITEM_ID = '0224184')) OR (ITM.ITEM_ID = '0224185')) OR (ITM.ITEM_ID = '0224188')) OR (ITM.ITEM_ID = '0224190')) OR (ITM.ITEM_ID = '0224203')) OR (ITM.ITEM_ID = '0224301')) OR (ITM.ITEM_ID = '0224302')) OR (ITM.ITEM_ID = '0224608')) OR (ITM.ITEM_ID = '0224997')) OR (ITM.ITEM_ID = '0225008')) OR (ITM.ITEM_ID = '0225012')) OR (ITM.ITEM_ID = '0226209')) OR (ITM.ITEM_ID = '0226376')) OR (ITM.ITEM_ID = '0226386')) OR (ITM.ITEM_ID = '0226424')) OR (ITM.ITEM_ID = '0227499')) OR (ITM.ITEM_ID = '0227550')) OR (ITM.ITEM_ID = '0229834')) OR (ITM.ITEM_ID = '0233620')) OR (ITM.ITEM_ID = '0233649')) OR (ITM.ITEM_ID = '0233659')) OR (ITM.ITEM_ID = '0235458')) OR (ITM.ITEM_ID = '0237289')) OR (ITM.ITEM_ID = '0237807')) OR (ITM.ITEM_ID = '0237808')) OR (ITM.ITEM_ID = '0238675')) OR (ITM.ITEM_ID = '0239021')) OR (ITM.ITEM_ID = '0239022')) OR (ITM.ITEM_ID = '0239023')) OR (ITM.ITEM_ID = '0239024')) OR (ITM.ITEM_ID = '0239025')) OR (ITM.ITEM_ID = '0244876')) OR (ITM.ITEM_ID = '0248015')) OR (ITM.ITEM_ID = '0248995')) OR (ITM.ITEM_ID = '0249702')) OR (ITM.ITEM_ID = '0249707')) OR (ITM.ITEM_ID = '0250131')) OR (ITM.ITEM_ID = '0250136')) OR (ITM.ITEM_ID = '0250837')) OR (ITM.ITEM_ID = '0251459')) OR (ITM.ITEM_ID = '0253778')) OR (ITM.ITEM_ID = '0254028')) OR (ITM.ITEM_ID = '0256796')) OR (ITM.ITEM_ID = '0257759')) OR (ITM.ITEM_ID = '0258263')) OR (ITM.ITEM_ID = '0259170')) OR (ITM.ITEM_ID = '0259260')) OR (ITM.ITEM_ID = '0259261')) OR (ITM.ITEM_ID = '0259262')) OR (ITM.ITEM_ID = '0259268')) OR (ITM.ITEM_ID = '0260047')) OR (ITM.ITEM_ID = '0262093')) OR (ITM.ITEM_ID = '0267128')) OR (ITM.ITEM_ID = '0267641')) OR (ITM.ITEM_ID = '0267643')) OR (ITM.ITEM_ID = '0281154')) OR (ITM.ITEM_ID = '0281156')) OR (ITM.ITEM_ID = '0281159')) OR (ITM.ITEM_ID = '0281227')) OR (ITM.ITEM_ID = '0294508')) OR (ITM.ITEM_ID = '0297232')) OR (ITM.ITEM_ID = '0297461')) OR (ITM.ITEM_ID = '0301742')) OR (ITM.ITEM_ID = '0310596')) OR (ITM.ITEM_ID = '0326662')) OR (ITM.ITEM_ID = '0326664')) OR (ITM.ITEM_ID = '0326666')) OR (ITM.ITEM_ID = '0326668')) OR (ITM.ITEM_ID = '0326670')) OR (ITM.ITEM_ID = '0326672')) OR (ITM.ITEM_ID = '0326676')) OR (ITM.ITEM_ID = '0326745')) OR (ITM.ITEM_ID = '0326828')) OR (ITM.ITEM_ID = '0326829')) OR (ITM.ITEM_ID = '0326831')) OR (ITM.ITEM_ID = '0326833')) OR (ITM.ITEM_ID = '0326836')) OR (ITM.ITEM_ID = '0326838')) OR (ITM.ITEM_ID = '0326839')) OR (ITM.ITEM_ID = '0326933')) OR (ITM.ITEM_ID = '0345841')) OR (ITM.ITEM_ID = '0345845')) OR (ITM.ITEM_ID = '0345847')) OR (ITM.ITEM_ID = '0345989')) OR (ITM.ITEM_ID = '0345998')) OR (ITM.ITEM_ID = '0409876')) OR (ITM.ITEM_ID = '0409877')) OR (ITM.ITEM_ID = '0424946')) OR (ITM.ITEM_ID = '430004 ')) OR (ITM.ITEM_ID = '430086 ')) OR (ITM.ITEM_ID = '430126 ')) OR (ITM.ITEM_ID = '0529668')) OR (ITM.ITEM_ID = '0529714')) OR (ITM.ITEM_ID = '0529800')) OR (ITM.ITEM_ID = '0529822')) OR (ITM.ITEM_ID = '0530096')) OR (ITM.ITEM_ID = '0530097')) OR (ITM.ITEM_ID = '0530106')) OR (ITM.ITEM_ID = '0530107')) OR (ITM.ITEM_ID = '0530114')) OR (ITM.ITEM_ID = '0530116')) OR (ITM.ITEM_ID = '0530117')) OR (ITM.ITEM_ID = '0530323')) OR (ITM.ITEM_ID = '0530487')) OR (ITM.ITEM_ID = '0530714')) OR (ITM.ITEM_ID = '580110 ')) OR (ITM.ITEM_ID = '580112 ')) OR (ITM.ITEM_ID = '584004 ')) OR (ITM.ITEM_ID = '0584008')) OR (ITM.ITEM_ID = '0584056')) OR (ITM.ITEM_ID = '0600000')) OR (ITM.ITEM_ID = '0600064')) OR (ITM.ITEM_ID = '0600071')) OR (ITM.ITEM_ID = '0600078')) OR (ITM.ITEM_ID = '0600125')) OR (ITM.ITEM_ID = '0600372')) OR (ITM.ITEM_ID = '0600405')) OR (ITM.ITEM_ID = '0600537')) OR (ITM.ITEM_ID = '0600750')) OR (ITM.ITEM_ID = '0600857')) OR (ITM.ITEM_ID = '0600905')) OR (ITM.ITEM_ID = '0600908')) OR (ITM.ITEM_ID = '0600966')) OR (ITM.ITEM_ID = '0600967')) OR (ITM.ITEM_ID = '0601171')) OR (ITM.ITEM_ID = '0601293')) OR (ITM.ITEM_ID = '0601414')) OR (ITM.ITEM_ID = '0601473')) OR (ITM.ITEM_ID = '0601475')) OR (ITM.ITEM_ID = '0601480')) OR (ITM.ITEM_ID = '0601482')) OR (ITM.ITEM_ID = '0601702')) OR (ITM.ITEM_ID = '0601762')) OR (ITM.ITEM_ID = '0601840')) OR (ITM.ITEM_ID = '0601843')) OR (ITM.ITEM_ID = '0601845')) OR (ITM.ITEM_ID = '0601847')) OR (ITM.ITEM_ID = '0601849')) OR (ITM.ITEM_ID = '0602090')) OR (ITM.ITEM_ID = '0602095')) OR (ITM.ITEM_ID = '0602099')) OR (ITM.ITEM_ID = '0602141')) OR (ITM.ITEM_ID = '0602223')) OR (ITM.ITEM_ID = '0602464')) OR (ITM.ITEM_ID = '0602937')) OR (ITM.ITEM_ID = '0603963')) OR (ITM.ITEM_ID = '0604021')) OR (ITM.ITEM_ID = '0604344')) OR (ITM.ITEM_ID = '0604424')) OR (ITM.ITEM_ID = '0604770')) OR (ITM.ITEM_ID = '0604771')) OR (ITM.ITEM_ID = '0604782')) OR (ITM.ITEM_ID = '0604848')) OR (ITM.ITEM_ID = '0604870')) OR (ITM.ITEM_ID = '0604937')) OR (ITM.ITEM_ID = '0604938')) OR (ITM.ITEM_ID = '0604940')) OR (ITM.ITEM_ID = '0604941')) OR (ITM.ITEM_ID = '0606188')) OR (ITM.ITEM_ID = '0606222')) OR (ITM.ITEM_ID = '0606229')) OR (ITM.ITEM_ID = '0606404')) OR (ITM.ITEM_ID = '0606407')) OR (ITM.ITEM_ID = '0606408')) OR (ITM.ITEM_ID = '0606599')) OR (ITM.ITEM_ID = '0608306')) OR (ITM.ITEM_ID = '0608449')) OR (ITM.ITEM_ID = '0608614')) OR (ITM.ITEM_ID = '0608640')) OR (ITM.ITEM_ID = '0609059')) OR (ITM.ITEM_ID = '0609522')) OR (ITM.ITEM_ID = '0609574')) OR (ITM.ITEM_ID = '0611196')) OR (ITM.ITEM_ID = '0611788')) OR (ITM.ITEM_ID = '0611979')) OR (ITM.ITEM_ID = '0612187')) OR (ITM.ITEM_ID = '0613000')) OR (ITM.ITEM_ID = '0613001')) OR (ITM.ITEM_ID = '0613290')) OR (ITM.ITEM_ID = '0613293')) OR (ITM.ITEM_ID = '0613296')) OR (ITM.ITEM_ID = '0614163')) OR (ITM.ITEM_ID = '0614184')) OR (ITM.ITEM_ID = '0616075')) OR (ITM.ITEM_ID = '0616097')) OR (ITM.ITEM_ID = '0616822')) OR (ITM.ITEM_ID = '0616823')) OR (ITM.ITEM_ID = '0616826')) OR (ITM.ITEM_ID = '0616827')) OR (ITM.ITEM_ID = '0616828')) OR (ITM.ITEM_ID = '0617068')) OR (ITM.ITEM_ID = '0617087')) OR (ITM.ITEM_ID = '0617265')) OR (ITM.ITEM_ID = '0617284')) OR (ITM.ITEM_ID = '0617399')) OR (ITM.ITEM_ID = '0617401')) OR (ITM.ITEM_ID = '0617406')) OR (ITM.ITEM_ID = '0617928')) OR (ITM.ITEM_ID = '0618236')) OR (ITM.ITEM_ID = '0618700')) OR (ITM.ITEM_ID = '0618703')) OR (ITM.ITEM_ID = '0618964')) OR (ITM.ITEM_ID = '0619463')) OR (ITM.ITEM_ID = '0619754')) OR (ITM.ITEM_ID = '0619869')) OR (ITM.ITEM_ID = '0619873')) OR (ITM.ITEM_ID = '0620315')) OR (ITM.ITEM_ID = '0620316')) OR (ITM.ITEM_ID = '0620320')) OR (ITM.ITEM_ID = '650113 ')) OR (ITM.ITEM_ID = '650143 ')) OR (ITM.ITEM_ID = '0660001')) OR (ITM.ITEM_ID = '0682337')) OR (ITM.ITEM_ID = '0682901')) OR (ITM.ITEM_ID = '0690870')) OR (ITM.ITEM_ID = '0690871')) OR (ITM.ITEM_ID = '0690872')) OR (ITM.ITEM_ID = '0690873')) OR (ITM.ITEM_ID = '0690890')) OR (ITM.ITEM_ID = '0690891')) OR (ITM.ITEM_ID = '0701504')) OR (ITM.ITEM_ID = '0709067')) OR (ITM.ITEM_ID = '0710590')) OR (ITM.ITEM_ID = '0710591')) OR (ITM.ITEM_ID = '0712315')) OR (ITM.ITEM_ID = '0713600')) OR (ITM.ITEM_ID = '0713636')) OR (ITM.ITEM_ID = '0713637')) OR (ITM.ITEM_ID = '0713638')) OR (ITM.ITEM_ID = '0713640')) OR (ITM.ITEM_ID = '0715845')) OR (ITM.ITEM_ID = '0717493')) OR (ITM.ITEM_ID = '0717531')) OR (ITM.ITEM_ID = '0719814')) OR (ITM.ITEM_ID = '0719817')) OR (ITM.ITEM_ID = '0719818')) OR (ITM.ITEM_ID = '0719859')) OR (ITM.ITEM_ID = '0720316')) OR (ITM.ITEM_ID = '0727268')) OR (ITM.ITEM_ID = '0727836')) OR (ITM.ITEM_ID = '0747384')) OR (ITM.ITEM_ID = '0747403')) OR (ITM.ITEM_ID = '0775462')) OR (ITM.ITEM_ID = '0800019')) OR (ITM.ITEM_ID = '0800205')) OR (ITM.ITEM_ID = '0800280')) OR (ITM.ITEM_ID = '0800500')) OR (ITM.ITEM_ID = '0800503')) OR (ITM.ITEM_ID = '0800540')) OR (ITM.ITEM_ID = '0805017')) OR (ITM.ITEM_ID = '0805598')) OR (ITM.ITEM_ID = '0806789')) OR (ITM.ITEM_ID = '0806940')) OR (ITM.ITEM_ID = '0806941')) OR (ITM.ITEM_ID = '0806942')) OR (ITM.ITEM_ID = '0806943')) OR (ITM.ITEM_ID = '0807119')) OR (ITM.ITEM_ID = '0808300')) OR (ITM.ITEM_ID = '0808302')) OR (ITM.ITEM_ID = '0808372')) OR (ITM.ITEM_ID = '0808483')) OR (ITM.ITEM_ID = '0811065')) OR (ITM.ITEM_ID = '0812264')) OR (ITM.ITEM_ID = '0812355')) OR (ITM.ITEM_ID = '0812357')) OR (ITM.ITEM_ID = '0812661')) OR (ITM.ITEM_ID = '0812687')) OR (ITM.ITEM_ID = '0812765')) OR (ITM.ITEM_ID = '0812847')) OR (ITM.ITEM_ID = '0814625')) OR (ITM.ITEM_ID = '0814626')) OR (ITM.ITEM_ID = '0814661')) OR (ITM.ITEM_ID = '0815012')) OR (ITM.ITEM_ID = '0815056')) OR (ITM.ITEM_ID = '0815528')) OR (ITM.ITEM_ID = '0815610')) OR (ITM.ITEM_ID = '0815944')) OR (ITM.ITEM_ID = '0815954')) OR (ITM.ITEM_ID = '0816052')) OR (ITM.ITEM_ID = '0816731')) OR (ITM.ITEM_ID = '0817748')) OR (ITM.ITEM_ID = '0819766')) OR (ITM.ITEM_ID = '0821221')) OR (ITM.ITEM_ID = '0822601')) OR (ITM.ITEM_ID = '0822602')) OR (ITM.ITEM_ID = '0822603')) OR (ITM.ITEM_ID = '0822704')) OR (ITM.ITEM_ID = '0824064')) OR (ITM.ITEM_ID = '0824747')) OR (ITM.ITEM_ID = '0826362')) OR (ITM.ITEM_ID = '0826632')) OR (ITM.ITEM_ID = '0826920')) OR (ITM.ITEM_ID = '0826922')) OR (ITM.ITEM_ID = '0831227')) OR (ITM.ITEM_ID = '0835709')) OR (ITM.ITEM_ID = '0836490')) OR (ITM.ITEM_ID = '0838471')) OR (ITM.ITEM_ID = '0838641')) OR (ITM.ITEM_ID = '0838642')) OR (ITM.ITEM_ID = '0838646')) OR (ITM.ITEM_ID = '0838653')) OR (ITM.ITEM_ID = '0840082')) OR (ITM.ITEM_ID = '0840464')) OR (ITM.ITEM_ID = '0844211')) OR (ITM.ITEM_ID = '0844216')) OR (ITM.ITEM_ID = '0851670')) OR (ITM.ITEM_ID = '0852582')) OR (ITM.ITEM_ID = '0853934')) OR (ITM.ITEM_ID = '0853967')) OR (ITM.ITEM_ID = '0853975')) OR (ITM.ITEM_ID = '0853984')) OR (ITM.ITEM_ID = '0854006')) OR (ITM.ITEM_ID = '0854165')) OR (ITM.ITEM_ID = '0858092')) OR (ITM.ITEM_ID = '0861562')) OR (ITM.ITEM_ID = '0867630')) OR (ITM.ITEM_ID = '0868457')) OR (ITM.ITEM_ID = '0877504')) OR (ITM.ITEM_ID = '0877515')) OR (ITM.ITEM_ID = '0878374')) OR (ITM.ITEM_ID = '0878384')) OR (ITM.ITEM_ID = '0878448')) OR (ITM.ITEM_ID = '0878495')) OR (ITM.ITEM_ID = '0878570')) OR (ITM.ITEM_ID = '0879488')) OR (ITM.ITEM_ID = '0879504')) OR (ITM.ITEM_ID = '0894002')) OR (ITM.ITEM_ID = '0894005')) OR (ITM.ITEM_ID = '0894021')) OR (ITM.ITEM_ID = '0894028')) OR (ITM.ITEM_ID = '1000407')) OR (ITM.ITEM_ID = '1000723')) OR (ITM.ITEM_ID = '1000844')) OR (ITM.ITEM_ID = '1000845')) OR (ITM.ITEM_ID = '1001866')) OR (ITM.ITEM_ID = '1001873')) OR (ITM.ITEM_ID = '1001889')) OR (ITM.ITEM_ID = '1001894')) OR (ITM.ITEM_ID = '1001895')) OR (ITM.ITEM_ID = '1001897')) OR (ITM.ITEM_ID = '1001898')) OR (ITM.ITEM_ID = '1001944')) OR (ITM.ITEM_ID = '1002043')) OR (ITM.ITEM_ID = '1002650')) OR (ITM.ITEM_ID = '1002864')) OR (ITM.ITEM_ID = '1003388')) OR (ITM.ITEM_ID = '1003389')) OR (ITM.ITEM_ID = '1005280')) OR (ITM.ITEM_ID = '1006189')) OR (ITM.ITEM_ID = '1006191')) OR (ITM.ITEM_ID = '1006223')) OR (ITM.ITEM_ID = '1006309')) OR (ITM.ITEM_ID = '1006668')) OR (ITM.ITEM_ID = '1006755')) OR (ITM.ITEM_ID = '1007002')) OR (ITM.ITEM_ID = '1007003')) OR (ITM.ITEM_ID = '1007005')) OR (ITM.ITEM_ID = '1007076')) OR (ITM.ITEM_ID = '1007251')) OR (ITM.ITEM_ID = '1007930')) OR (ITM.ITEM_ID = '1008338')) OR (ITM.ITEM_ID = '1008341')) OR (ITM.ITEM_ID = '1008541')) OR (ITM.ITEM_ID = '1008542')) OR (ITM.ITEM_ID = '1009005')) OR (ITM.ITEM_ID = '1010010')) OR (ITM.ITEM_ID = '1010356')) OR (ITM.ITEM_ID = '1010413')) OR (ITM.ITEM_ID = '1010414')) OR (ITM.ITEM_ID = '1011196')) OR (ITM.ITEM_ID = '1011197')) OR (ITM.ITEM_ID = '1011540')) OR (ITM.ITEM_ID = '1011971')) OR (ITM.ITEM_ID = '1012197')) OR (ITM.ITEM_ID = '1012345')) OR (ITM.ITEM_ID = '1013502')) OR (ITM.ITEM_ID = '1013680')) OR (ITM.ITEM_ID = '1014251')) OR (ITM.ITEM_ID = '1014346')) OR (ITM.ITEM_ID = '1014348')) OR (ITM.ITEM_ID = '1014352')) OR (ITM.ITEM_ID = '1014358')) OR (ITM.ITEM_ID = '1014413')) OR (ITM.ITEM_ID = '1014522')) OR (ITM.ITEM_ID = '1014523')) OR (ITM.ITEM_ID = '1016040')) OR (ITM.ITEM_ID = '1016913')) OR (ITM.ITEM_ID = '1017215')) OR (ITM.ITEM_ID = '1017222')) OR (ITM.ITEM_ID = '1017223')) OR (ITM.ITEM_ID = '1017224')) OR (ITM.ITEM_ID = '1017225')) OR (ITM.ITEM_ID = '1017249')) OR (ITM.ITEM_ID = '1017351')) OR (ITM.ITEM_ID = '1017966')) OR (ITM.ITEM_ID = '1019543')) OR (ITM.ITEM_ID = '1023045')) OR (ITM.ITEM_ID = '1024282')) OR (ITM.ITEM_ID = '1024344')) OR (ITM.ITEM_ID = '1024431')) OR (ITM.ITEM_ID = '1024701')) OR (ITM.ITEM_ID = '1024702')) OR (ITM.ITEM_ID = '1024704')) OR (ITM.ITEM_ID = '1024793')) OR (ITM.ITEM_ID = '1024818')) OR (ITM.ITEM_ID = '1024820')) OR (ITM.ITEM_ID = '1025723')) OR (ITM.ITEM_ID = '1025724')) OR (ITM.ITEM_ID = '1025725')) OR (ITM.ITEM_ID = '1025731')) OR (ITM.ITEM_ID = '1025732')) OR (ITM.ITEM_ID = '1025733')) OR (ITM.ITEM_ID = '1027415')) OR (ITM.ITEM_ID = '1028541')) OR (ITM.ITEM_ID = '1029616')) OR (ITM.ITEM_ID = '1029618')) OR (ITM.ITEM_ID = '1033393')) OR (ITM.ITEM_ID = '1043180')) OR (ITM.ITEM_ID = '1046232')) OR (ITM.ITEM_ID = '1046233')) OR (ITM.ITEM_ID = '1046234')) OR (ITM.ITEM_ID = '1046405')) OR (ITM.ITEM_ID = '1047364')) OR (ITM.ITEM_ID = '1047366')) OR (ITM.ITEM_ID = '1047369')) OR (ITM.ITEM_ID = '1047370')) OR (ITM.ITEM_ID = '1047965')) OR (ITM.ITEM_ID = '1048965')) OR (ITM.ITEM_ID = '1049336')) OR (ITM.ITEM_ID = '1049936')) OR (ITM.ITEM_ID = '1050017')) OR (ITM.ITEM_ID = '1050020')) OR (ITM.ITEM_ID = '1050021')) OR (ITM.ITEM_ID = '1050022')) OR (ITM.ITEM_ID = '1050024')) OR (ITM.ITEM_ID = '1050025')) OR (ITM.ITEM_ID = '1050041')) OR (ITM.ITEM_ID = '1050042')) OR (ITM.ITEM_ID = '1050044')) OR (ITM.ITEM_ID = '1050045')) OR (ITM.ITEM_ID = '1050046')) OR (ITM.ITEM_ID = '1050047')) OR (ITM.ITEM_ID = '1050069')) OR (ITM.ITEM_ID = '1050072')) OR (ITM.ITEM_ID = '1050078')) OR (ITM.ITEM_ID = '1050079')) OR (ITM.ITEM_ID = '1050123')) OR (ITM.ITEM_ID = '1050142')) OR (ITM.ITEM_ID = '1050145')) OR (ITM.ITEM_ID = '1050159')) OR (ITM.ITEM_ID = '1050161')) OR (ITM.ITEM_ID = '1050162')) OR (ITM.ITEM_ID = '1050163')) OR (ITM.ITEM_ID = '1050560')) OR (ITM.ITEM_ID = '1051217')) OR (ITM.ITEM_ID = '1051241')) OR (ITM.ITEM_ID = '1051302')) OR (ITM.ITEM_ID = '1051303')) OR (ITM.ITEM_ID = '1051304')) OR (ITM.ITEM_ID = '1051305')) OR (ITM.ITEM_ID = '1053007')) OR (ITM.ITEM_ID = '1053192')) OR (ITM.ITEM_ID = '1054100')) OR (ITM.ITEM_ID = '1054257')) OR (ITM.ITEM_ID = '1054259')) OR (ITM.ITEM_ID = '1054260')) OR (ITM.ITEM_ID = '1054434')) OR (ITM.ITEM_ID = '1056239')) OR (ITM.ITEM_ID = '1056429')) OR (ITM.ITEM_ID = '1056431')) OR (ITM.ITEM_ID = '1056439')) OR (ITM.ITEM_ID = '1056623')) OR (ITM.ITEM_ID = '1068102')) OR (ITM.ITEM_ID = '1089000')) OR (ITM.ITEM_ID = '1089020')) OR (ITM.ITEM_ID = '1089060')) OR (ITM.ITEM_ID = '1090608')) OR (ITM.ITEM_ID = '1130825')) OR (ITM.ITEM_ID = '1131951')) OR (ITM.ITEM_ID = '1136106')) OR (ITM.ITEM_ID = '1136110')) OR (ITM.ITEM_ID = '1136114')) OR (ITM.ITEM_ID = '1137965')) OR (ITM.ITEM_ID = '1153017')) OR (ITM.ITEM_ID = '1153021')) OR (ITM.ITEM_ID = '1153025')) OR (ITM.ITEM_ID = '1153033')) OR (ITM.ITEM_ID = '1301645')) OR (ITM.ITEM_ID = '1301646')) OR (ITM.ITEM_ID = '1301647')) OR (ITM.ITEM_ID = '1301648')) OR (ITM.ITEM_ID = '1307446')) OR (ITM.ITEM_ID = '1307447')) OR (ITM.ITEM_ID = '1307448')) OR (ITM.ITEM_ID = '1307449')) OR (ITM.ITEM_ID = '1323390')) OR (ITM.ITEM_ID = '1327172')) OR (ITM.ITEM_ID = '1327173')) OR (ITM.ITEM_ID = '1327174')) OR (ITM.ITEM_ID = '1327175')) OR (ITM.ITEM_ID = '1327176')) OR (ITM.ITEM_ID = '1327280')) OR (ITM.ITEM_ID = '1329833')) OR (ITM.ITEM_ID = '1330199')) OR (ITM.ITEM_ID = '1333624')) OR (ITM.ITEM_ID = '1333625')) OR (ITM.ITEM_ID = '1333626')) OR (ITM.ITEM_ID = '1333627')) OR (ITM.ITEM_ID = '1334808')) OR (ITM.ITEM_ID = '1334809')) OR (ITM.ITEM_ID = '1334810')) OR (ITM.ITEM_ID = '1334811')) OR (ITM.ITEM_ID = '1334813')) OR (ITM.ITEM_ID = '1334814')) OR (ITM.ITEM_ID = '1334815')) OR (ITM.ITEM_ID = '1334818')) OR (ITM.ITEM_ID = '1334819')) OR (ITM.ITEM_ID = '1334820')) OR (ITM.ITEM_ID = '1335593')) OR (ITM.ITEM_ID = '1602152')) OR (ITM.ITEM_ID = '1602179')) OR (ITM.ITEM_ID = '1614539')) OR (ITM.ITEM_ID = '1614554')) OR (ITM.ITEM_ID = '1614562')) OR (ITM.ITEM_ID = '1614564')) OR (ITM.ITEM_ID = '1614595')) OR (ITM.ITEM_ID = '1615241')) OR (ITM.ITEM_ID = '1615242')) OR (ITM.ITEM_ID = '1615243')) OR (ITM.ITEM_ID = '1615252')) OR (ITM.ITEM_ID = '1615259')) OR (ITM.ITEM_ID = '1615260')) OR (ITM.ITEM_ID = '1615261')) OR (ITM.ITEM_ID = '1615267')) OR (ITM.ITEM_ID = '1615270')) OR (ITM.ITEM_ID = '1615271')) OR (ITM.ITEM_ID = '1615274')) OR (ITM.ITEM_ID = '1615275')) OR (ITM.ITEM_ID = '1615277')) OR (ITM.ITEM_ID = '1615283')) OR (ITM.ITEM_ID = '1615459')) OR (ITM.ITEM_ID = '1615461')) OR (ITM.ITEM_ID = '1615462')) OR (ITM.ITEM_ID = '1615480')) OR (ITM.ITEM_ID = '1616523')) OR (ITM.ITEM_ID = '1616524')) OR (ITM.ITEM_ID = '1616525')) OR (ITM.ITEM_ID = '1616526')) OR (ITM.ITEM_ID = '1616527')) OR (ITM.ITEM_ID = '1616640')) OR (ITM.ITEM_ID = '1616646')) OR (ITM.ITEM_ID = '1617312')) OR (ITM.ITEM_ID = '1617567')) OR (ITM.ITEM_ID = '1617568')) OR (ITM.ITEM_ID = '1617575')) OR (ITM.ITEM_ID = '1617578')) OR (ITM.ITEM_ID = '1619451')) OR (ITM.ITEM_ID = '1619882')) OR (ITM.ITEM_ID = '2105085')) OR (ITM.ITEM_ID = '2105204')) OR (ITM.ITEM_ID = '2105209')) OR (ITM.ITEM_ID = '2105210')) OR (ITM.ITEM_ID = '2105211')) OR (ITM.ITEM_ID = '2105212')) OR (ITM.ITEM_ID = '2105215')) OR (ITM.ITEM_ID = '2105217')) OR (ITM.ITEM_ID = '2105218')) OR (ITM.ITEM_ID = '2105219')) OR (ITM.ITEM_ID = '2105221')) OR (ITM.ITEM_ID = '2112074')) OR (ITM.ITEM_ID = '2112077')) OR (ITM.ITEM_ID = '2112078')) OR (ITM.ITEM_ID = '2112080')) OR (ITM.ITEM_ID = '2112081')) OR (ITM.ITEM_ID = '2112082')) OR (ITM.ITEM_ID = '2112083')) OR (ITM.ITEM_ID = '2112085')) OR (ITM.ITEM_ID = '2112086')) OR (ITM.ITEM_ID = '2112087')) OR (ITM.ITEM_ID = '2112088')) OR (ITM.ITEM_ID = '2112115')) OR (ITM.ITEM_ID = '2112118')) OR (ITM.ITEM_ID = '2112119')) OR (ITM.ITEM_ID = '2112121')) OR (ITM.ITEM_ID = '2112122')) OR (ITM.ITEM_ID = '2112151')) OR (ITM.ITEM_ID = '2112152')) OR (ITM.ITEM_ID = '2112153')) OR (ITM.ITEM_ID = '2112421')) OR (ITM.ITEM_ID = '2115714')) OR (ITM.ITEM_ID = '2115715')) OR (ITM.ITEM_ID = '2116834')) OR (ITM.ITEM_ID = '2119983')) OR (ITM.ITEM_ID = '2119986')) OR (ITM.ITEM_ID = '2119987')) OR (ITM.ITEM_ID = '2119988')) OR (ITM.ITEM_ID = '2121694')) OR (ITM.ITEM_ID = '2127257')) OR (ITM.ITEM_ID = '2127263')) OR (ITM.ITEM_ID = '2127269')) OR (ITM.ITEM_ID = '2127279')) OR (ITM.ITEM_ID = '2127286')) OR (ITM.ITEM_ID = '2127292')) OR (ITM.ITEM_ID = '2127335')) OR (ITM.ITEM_ID = '2127336')) OR (ITM.ITEM_ID = '2127339')) OR (ITM.ITEM_ID = '2127341')) OR (ITM.ITEM_ID = '2127343')) OR (ITM.ITEM_ID = '2127344')) OR (ITM.ITEM_ID = '2127346')) OR (ITM.ITEM_ID = '2135936')) OR (ITM.ITEM_ID = '2135940')) OR (ITM.ITEM_ID = '2140180')) OR (ITM.ITEM_ID = '2140183')) OR (ITM.ITEM_ID = '2140528')) OR (ITM.ITEM_ID = '2141781')) OR (ITM.ITEM_ID = '2161704')) OR (ITM.ITEM_ID = '2161727')) OR (ITM.ITEM_ID = '2163055')) OR (ITM.ITEM_ID = '2163056')) OR (ITM.ITEM_ID = '2169054')) OR (ITM.ITEM_ID = '3163183')) OR (ITM.ITEM_ID = '3238252')) OR (ITM.ITEM_ID = '3238253')) OR (ITM.ITEM_ID = '3276231')) OR (ITM.ITEM_ID = '4134674')) OR (ITM.ITEM_ID = '4203593')) OR (ITM.ITEM_ID = '4203599')) OR (ITM.ITEM_ID = '5100008')) OR (ITM.ITEM_ID = '5100148')) OR (ITM.ITEM_ID = '5100297')) OR (ITM.ITEM_ID = '5100316')) OR (ITM.ITEM_ID = '5100638')) OR (ITM.ITEM_ID = '5100651')) OR (ITM.ITEM_ID = '5126656')) OR (ITM.ITEM_ID = '7000023')) OR (ITM.ITEM_ID = '7000078')) OR (ITM.ITEM_ID = '7003449')) OR (ITM.ITEM_ID = '7005120')) OR (ITM.ITEM_ID = '7011669')) OR (ITM.ITEM_ID = '11562825')) OR (ITM.ITEM_ID = '11562826')) OR (ITM.ITEM_ID = '11562827')) OR (ITM.ITEM_ID = '11620748')) OR (ITM.ITEM_ID = '91000498')) OR (ITM.ITEM_ID = '91096878')) OR (ITM.ITEM_ID = '91129889')) OR (ITM.ITEM_ID = '91182449')) OR (ITM.ITEM_ID = '99110239')) OR (ITM.ITEM_ID = '99110275')) OR (ITM.ITEM_ID = '99110975')) OR (ITM.ITEM_ID = '99249806')) OR (ITM.ITEM_ID = '99331762')) OR (ITM.ITEM_ID = '99331764')) OR (ITM.ITEM_ID = '99331765')) OR (ITM.ITEM_ID = '99332898')) OR (ITM.ITEM_ID = '99332908')) OR (ITM.ITEM_ID = '99332924')) OR (ITM.ITEM_ID = '99381747')) OR (ITM.ITEM_ID = '99393236')) OR (ITM.ITEM_ID = '99448475')) OR (ITM.ITEM_ID = '99448682')) OR (ITM.ITEM_ID = '99474912')) OR (ITM.ITEM_ID = '99474913')) OR (ITM.ITEM_ID = '99475900')) OR (ITM.ITEM_ID = '99475902')) OR (ITM.ITEM_ID = '99490046')) OR (ITM.ITEM_ID = '99492156')) OR (ITM.ITEM_ID = '99499106')) OR (ITM.ITEM_ID = '99499449')) OR (ITM.ITEM_ID = '99529348')) OR (ITM.ITEM_ID = '99541174')) OR (ITM.ITEM_ID = '99541177')) OR (ITM.ITEM_ID = '99569139')) OR (ITM.ITEM_ID = '99592286')) OR (ITM.ITEM_ID = '99595195')) OR (ITM.ITEM_ID = '99869602')) OR (ITM.ITEM_ID = '99874945')) OR (ITM.ITEM_ID = '99875894')) OR (ITM.ITEM_ID = '99887729')) OR (ITM.ITEM_ID = '110120306')) OR (ITM.ITEM_ID = '110142394')) OR (ITM.ITEM_ID = '110345838')) OR (ITM.ITEM_ID = '110345841')) OR (ITM.ITEM_ID = '110345843')) OR (ITM.ITEM_ID = '110345845')) OR (ITM.ITEM_ID = '110345847')) OR (ITM.ITEM_ID = '110345849')) OR (ITM.ITEM_ID = '110345850')) OR (ITM.ITEM_ID = '110345857')) OR (ITM.ITEM_ID = '110600273')) OR (ITM.ITEM_ID = '110600274')) OR (ITM.ITEM_ID = '110600275')) OR (ITM.ITEM_ID = '111000815')) OR (ITM.ITEM_ID = '111001990')) OR (ITM.ITEM_ID = '111002661')) OR (ITM.ITEM_ID = '111002820')) OR (ITM.ITEM_ID = '111002821')) OR (ITM.ITEM_ID = '111002823')) OR (ITM.ITEM_ID = '111005627')) OR (ITM.ITEM_ID = '111017247')) OR (ITM.ITEM_ID = '111024774')) OR (ITM.ITEM_ID = '111024775')) OR (ITM.ITEM_ID = '111024776')) OR (ITM.ITEM_ID = '111024789')) OR (ITM.ITEM_ID = '111024792')) OR (ITM.ITEM_ID = '111024793')) OR (ITM.ITEM_ID = '111024794')) OR (ITM.ITEM_ID = '111024795')) OR (ITM.ITEM_ID = '111024796')) OR (ITM.ITEM_ID = '111024799')) OR (ITM.ITEM_ID = '111024800')) OR (ITM.ITEM_ID = '111024807')) OR (ITM.ITEM_ID = '111024813')) OR (ITM.ITEM_ID = '111024818')) OR (ITM.ITEM_ID = '111024819')) OR (ITM.ITEM_ID = '111024820')) OR (ITM.ITEM_ID = '111050163')) OR (ITM.ITEM_ID = '113238680')) OR (ITM.ITEM_ID = '113239069')) OR (ITM.ITEM_ID = '113239070')) OR (ITM.ITEM_ID = '113239072')) OR (ITM.ITEM_ID = '113239074')) OR (ITM.ITEM_ID = '113239076')) OR (ITM.ITEM_ID = '113239080')) OR (ITM.ITEM_ID = '113239084')) OR (ITM.ITEM_ID = '113239088')) OR (ITM.ITEM_ID = '113239096')) OR (ITM.ITEM_ID = '121001940')) OR (ITM.ITEM_ID = '121049936')) OR (ITM.ITEM_ID = '150812645')) OR (ITM.ITEM_ID = '150812646')) OR (ITM.ITEM_ID = '920092794')) OR (ITM.ITEM_ID = '02558230067')) OR (ITM.ITEM_ID = '02558230127')) OR (ITM.ITEM_ID = '02558230167')) OR (ITM.ITEM_ID = '03854821003')) OR (ITM.ITEM_ID = '0202901V01'));";

    // When:
    makeRequest(firstStatement);
    final KsqlErrorMessage result =
        makeFailingRequest(secondStatement, BAD_REQUEST.code());

    // Then:
    assertThat(result.getErrorCode(), is(Errors.ERROR_CODE_BAD_STATEMENT));
    assertThat(result.getMessage(),
        containsString("Statement is too large to parse. "
            + "This may be caused by having too many nested expressions in the statement."));
    assertThat(((KsqlStatementErrorMessage) result).getStatementText(), is(secondStatement));
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
    assertThat(response.getEntity(), instanceOf(KsqlStatementErrorMessage.class));
    final KsqlStatementErrorMessage entity = (KsqlStatementErrorMessage) response.getEntity();
    assertThat(entity.getStatementText(), containsString("TERMINATE CLUSTER"));
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
        "CREATE STREAM SOURCE (val int) WITH (kafka_topic='topic2', key_format='kafka', value_format='json');";

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
        "CREATE TABLE SOURCE (id int primary key, val int) WITH (kafka_topic='topic2', key_format='kafka', value_format='json');";

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
    assertThat(e.getResponse().getEntity(), instanceOf(KsqlStatementErrorMessage.class));
    final KsqlStatementErrorMessage entity = (KsqlStatementErrorMessage) e.getResponse().getEntity();
    assertThat(entity.getStatementText(), containsString(statement));
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
        table.getKsqlTopic().getKeyFormat().getFormat(),
        table.getKsqlTopic().getValueFormat().getFormat(),
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
        stream.getKsqlTopic().getKeyFormat().getFormat(),
        stream.getKsqlTopic().getValueFormat().getFormat(),
        stream.getKsqlTopic().getKeyFormat().isWindowed()
    );
  }

  private void givenMockEngine() {
    ksqlEngine = mock(KsqlEngine.class);
    when(ksqlEngine.parse(any()))
        .thenAnswer(invocation -> realEngine.parse(invocation.getArgument(0)));
    when(ksqlEngine.prepare(any(), any()))
        .thenAnswer(invocation ->
            realEngine.prepare(invocation.getArgument(0), Collections.emptyMap()));
    when(sandbox.prepare(any(), any()))
        .thenAnswer(invocation ->
            realEngine.createSandbox(serviceContext)
                .prepare(invocation.getArgument(0), Collections.emptyMap()));
    when(sandbox.plan(any(), any())).thenAnswer(
        i -> KsqlPlan.ddlPlanCurrent(
            ((ConfiguredStatement<?>) i.getArgument(1)).getMaskedStatementText(),
            new DropSourceCommand(SourceName.of("bob"))
        )
    );
    when(ksqlEngine.getKsqlConfig()).thenReturn(ksqlConfig);
    when(sandbox.getKsqlConfig()).thenReturn(ksqlConfig);
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
            md.getSinkName().isPresent()
                ? ImmutableSet.of(md.getSinkName().get().text())
                : ImmutableSet.of(),
            md.getResultTopic().isPresent()
                ? ImmutableSet.of(md.getResultTopic().get().getKafkaTopicName())
                : ImmutableSet.of(),
            md.getQueryId(),
            new QueryStatusCount(Collections.singletonMap(KsqlConstants.fromStreamsState(md.getState()), 1)),
            KsqlConstants.KsqlQueryType.PERSISTENT)
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
        denyListPropertyValidator,
        commandRunnerWarning
    );

    ksqlResource.configure(ksqlConfig);
  }

  @Test
  public void shouldReturnBadRequestWhenIsValidatorIsCalledWithProhibitedProps() {
    final Map<String, Object> properties = new HashMap<>();
    properties.put("ksql.service.id", "");

    // Given:
    doThrow(new KsqlException("deny override")).when(denyListPropertyValidator).validateAll(
        properties
    );

    // When:
    final EndpointResponse response = ksqlResource.isValidProperty("ksql.service.id");

    // Then:
    assertThat(response.getStatus(), equalTo(400));
  }

  @Test
  public void shouldReturnBadRequestWhenIsValidatorIsCalledWithNonQueryLevelProps() {
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "");
    givenKsqlConfigWith(ImmutableMap.of(
        KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true
    ));

    // When:
    final EndpointResponse response = ksqlResource.isValidProperty("ksql.service.id");

    // Then:
    assertThat(response.getStatus(), equalTo(400));
  }

  @Test
  public void shouldNotBadRequestWhenIsValidatorIsCalledWithNonQueryLevelProps() {
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "");
    givenKsqlConfigWith(ImmutableMap.of(
        KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, true
    ));

    // When:
    final EndpointResponse response = ksqlResource.isValidProperty("ksql.streams.auto.offset.reset");

    // Then:
    assertThat(response.getStatus(), equalTo(200));
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
  public void queryLoggerShouldReceiveStatementsWhenHandleKsqlStatement() {
    try (MockedStatic<QueryLogger> logger = Mockito.mockStatic(QueryLogger.class)) {
      ksqlResource.handleKsqlStatements(securityContext, VALID_EXECUTABLE_REQUEST);

      logger.verify(() -> QueryLogger.info("Query created",
          VALID_EXECUTABLE_REQUEST.getMaskedKsql()), times(1));
    }
  }

  @Test
  public void queryLoggerShouldReceiveTerminateStatementsWhenHandleKsqlStatementWithTerminate() {
    // Given:
    final PersistentQueryMetadata queryMetadata = createQuery(
        "CREATE STREAM test_explain AS SELECT * FROM test_stream;",
        emptyMap());
    final String terminateSql = "TERMINATE " + queryMetadata.getQueryId() + ";";

    // When:
    try (MockedStatic<QueryLogger> logger = Mockito.mockStatic(QueryLogger.class)) {
      ksqlResource.handleKsqlStatements(securityContext, new KsqlRequest(
          terminateSql,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
          emptyMap(),
          0L));

      // Then:
      logger.verify(() -> QueryLogger.info("Query terminated",
          terminateSql), times(1));
    }
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
        denyListPropertyValidator,
        commandRunnerWarning
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
    final MetricCollectors metricCollectors = new MetricCollectors();
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore,
        (engine) -> new KsqlEngineMetrics("", engine, Collections.emptyMap(), Optional.empty(),
            metricCollectors
        ),
        new SequentialQueryIdGenerator(),
        ksqlConfig,
        metricCollectors
    );

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
    givenSource(type, sourceName, topicName, schema, Collections.emptySet());
  }

  private void givenSource(
      final DataSourceType type,
      final String sourceName,
      final String topicName,
      final LogicalSchema schema,
      final Set<SourceName> sourceReferences
  ) {
    final KsqlTopic ksqlTopic = new KsqlTopic(
        topicName,
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
    );

    givenKafkaTopicExists(topicName);
    final DataSource source;
    switch (type) {
      case KSTREAM:
        source = new KsqlStream<>(
            "statementText",
            SourceName.of(sourceName),
            schema,
            Optional.empty(),
            false,
            ksqlTopic,
            false
        );
        break;
      case KTABLE:
        source = new KsqlTable<>(
            "statementText",
            SourceName.of(sourceName),
            schema,
            Optional.empty(),
            false,
            ksqlTopic,
            false
        );
        break;
      default:
        throw new IllegalArgumentException(type.toString());
    }

    metaStore.putSource(source, false);
    metaStore.addSourceReferences(source.getName(), sourceReferences);
  }

  private static Properties getDefaultKsqlConfig() {
    final Map<String, Object> configMap = new HashMap<>(KsqlConfigTestUtil.baseTestConfig());
    configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configMap.put("ksql.command.topic.suffix", "commands");
    configMap.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088");
    configMap.put(StreamsConfig.APPLICATION_SERVER_CONFIG, APPLICATION_SERVER);
    configMap.put(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG, "KAFKA");

    final Properties properties = new Properties();
    properties.putAll(configMap);

    return properties;
  }

  private static void registerValueSchema(final SchemaRegistryClient schemaRegistryClient)
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
    schemaRegistryClient.register(KsqlConstants.getSRSubject("orders-topic", false),
        new AvroSchema(avroSchema));
  }

  private static Matcher<CommandId> commandIdWithString(final String commandIdStr) {
    return new TypeSafeMatcher<CommandId>() {
      @Override
      protected boolean matchesSafely(final CommandId commandId) {
        return commandId.toString().equals(commandIdStr);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(commandIdStr);
      }
    };
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
