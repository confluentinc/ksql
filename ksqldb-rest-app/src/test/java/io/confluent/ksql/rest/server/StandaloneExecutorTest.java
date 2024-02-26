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

package io.confluent.ksql.rest.server;

import static io.confluent.ksql.parser.ParserMatchers.configured;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.function.UserFunctionLoader;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class StandaloneExecutorTest {

  private static final String PROCESSING_LOG_TOPIC_NAME = "proclogtop";
  private static final ProcessingLogConfig processingLogConfig =
      new ProcessingLogConfig(ImmutableMap.of(
          ProcessingLogConfig.TOPIC_AUTO_CREATE, true,
          ProcessingLogConfig.TOPIC_NAME, PROCESSING_LOG_TOPIC_NAME
      ));
  private static final KsqlConfig ksqlConfig = new KsqlConfig(
      ImmutableMap.of(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
  );

  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(ColumnName.of("bob"), new Type(SqlTypes.STRING)));

  private static final SourceName SOME_NAME = SourceName.of("Bob");
  private static final String SOME_TOPIC = "some-topic";

  private static final RefinementInfo REFINEMENT_INFO =
      RefinementInfo.of(OutputRefinement.CHANGES);

  private static final CreateSourceProperties JSON_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          "VALUE_FORMAT", new StringLiteral("json"),
          "KAFKA_TOPIC", new StringLiteral(SOME_TOPIC)
      )
  );

  private static final CreateSourceProperties AVRO_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          "VALUE_FORMAT", new StringLiteral("avro"),
          "KAFKA_TOPIC", new StringLiteral(SOME_TOPIC))
  );

  private static final CreateStream CREATE_STREAM = new CreateStream(
      SOME_NAME, SOME_ELEMENTS, false, true, JSON_PROPS, false);

  private static final CreateStreamAsSelect CREATE_STREAM_AS_SELECT = new CreateStreamAsSelect(
      SourceName.of("stream"),
      new Query(
          Optional.empty(),
          new Select(ImmutableList.of(new AllColumns(Optional.empty()))),
          new Table(SourceName.of("sink")),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.of(REFINEMENT_INFO),
          false,
          OptionalInt.empty()
      ),
      false,
      false,
      CreateSourceAsProperties.none()
  );


  private final static ParsedStatement PARSED_STMT_0 = ParsedStatement
      .of("sql 0", mock(SingleStatementContext.class));

  private final static ParsedStatement PARSED_STMT_1 = ParsedStatement
      .of("sql 1", mock(SingleStatementContext.class));

  private final static PreparedStatement<?> PREPARED_STMT_0 = PreparedStatement
      .of("sql 0", CREATE_STREAM);

  private final static PreparedStatement<?> PREPARED_STMT_1 = PreparedStatement
      .of("sql 1", CREATE_STREAM);

  private final static PreparedStatement<?> PREPARED_CSAS = PreparedStatement.
      of("CSAS", CREATE_STREAM_AS_SELECT);

  private final static ConfiguredStatement<?> CFG_STMT_0 = ConfiguredStatement
      .of(PREPARED_STMT_0, SessionConfig.of(ksqlConfig, emptyMap()));

  private final static ConfiguredStatement<?> CFG_STMT_1 = ConfiguredStatement
      .of(PREPARED_STMT_1, SessionConfig.of(ksqlConfig, emptyMap()));

  private final static PreparedStatement<CreateStream> STMT_0_WITH_SCHEMA = PreparedStatement
      .of("sql 0", new CreateStream(
          SourceName.of("CS 0"),
          SOME_ELEMENTS,
          false,
          true,
          JSON_PROPS,
          false
      ));

  private final static ConfiguredStatement<?> CFG_0_WITH_SCHEMA = ConfiguredStatement
      .of(STMT_0_WITH_SCHEMA, SessionConfig.of(ksqlConfig, emptyMap()));

  private final static PreparedStatement<CreateStream> STMT_1_WITH_SCHEMA = PreparedStatement
      .of("sql 1", new CreateStream(
          SourceName.of("CS 1"),
          SOME_ELEMENTS,
          false,
          true,
          JSON_PROPS,
          false
      ));

  private final static ConfiguredStatement<?> CFG_1_WITH_SCHEMA = ConfiguredStatement
      .of(STMT_1_WITH_SCHEMA, SessionConfig.of(ksqlConfig, emptyMap()));


  private final static PreparedStatement<CreateStreamAsSelect> CSAS_WITH_TOPIC = PreparedStatement
      .of("CSAS_TOPIC", CREATE_STREAM_AS_SELECT);

  private final static ConfiguredStatement<CreateStreamAsSelect> CSAS_CFG_WITH_TOPIC =
      ConfiguredStatement
          .of(CSAS_WITH_TOPIC, SessionConfig.of(ksqlConfig, emptyMap()));

  @Mock
  private Query query;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlExecutionContext sandBox;
  @Mock
  private UserFunctionLoader udfLoader;
  @Mock
  private PersistentQueryMetadata persistentQuery;
  @Mock
  private PersistentQueryMetadata sandBoxQuery;
  @Mock
  private QueryMetadata nonPersistentQueryMd;
  @Mock
  private VersionCheckerAgent versionChecker;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ServiceContext sandBoxServiceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private BiFunction<KsqlExecutionContext, ServiceContext, Injector> injectorFactory;
  @Mock
  private Injector schemaInjector;
  @Mock
  private Injector sandBoxSchemaInjector;
  @Mock
  private Injector topicInjector;
  @Mock
  private Injector sandBoxTopicInjector;
  @Mock
  private Consumer<KsqlConfig> rocksDBConfigSetterHandler;

  private Path queriesFile;
  private StandaloneExecutor standaloneExecutor;


  @Before
  public void before() throws Exception {
    queriesFile = Paths.get(TestUtils.tempFile().getPath());
    givenQueryFileContains("something");

    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);

    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0));

    when(ksqlEngine.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(ksqlEngine.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(ksqlEngine.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of(persistentQuery));

    when(ksqlEngine.createSandbox(any())).thenReturn(sandBox);

    when(sandBox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandBox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(sandBox.getServiceContext()).thenReturn(sandBoxServiceContext);
    when(sandBox.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of("success"));
    when(sandBox.execute(sandBoxServiceContext, CSAS_CFG_WITH_TOPIC))
        .thenReturn(ExecuteResult.of(persistentQuery));

    when(injectorFactory.apply(any(), any())).thenReturn(InjectorChain.of(sandBoxSchemaInjector, sandBoxTopicInjector));
    when(injectorFactory.apply(ksqlEngine, serviceContext)).thenReturn(InjectorChain.of(schemaInjector, topicInjector));

    when(sandBoxSchemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    when(sandBoxTopicInjector.inject(any()))
        .thenAnswer(inv -> inv.getArgument(0));
    when(topicInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        false,
        versionChecker,
        injectorFactory,
        new MetricCollectors(),
        rocksDBConfigSetterHandler
    );
  }

  @Test
  public void shouldStartTheVersionCheckerAgent() {
    // When:
    standaloneExecutor.startAsync();

    verify(versionChecker).start(eq(KsqlModuleType.SERVER), any());
  }

  @Test
  public void shouldAddConfigurableMetricsReportersIfPresentInKsqlConfig() {
    // When:
    final MetricsReporter mockReporter = mock(MetricsReporter.class);
    final KsqlConfig mockKsqlConfig = mock(KsqlConfig.class);
    when(mockKsqlConfig.getConfiguredInstances(anyString(), any(), any()))
        .thenReturn(Collections.singletonList(mockReporter));
    when(mockKsqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)).thenReturn("ksql-id");
    final MetricCollectors metricCollectors = new MetricCollectors();
    standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        processingLogConfig,
        mockKsqlConfig,
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        false,
        versionChecker,
        injectorFactory,
        metricCollectors,
        rocksDBConfigSetterHandler
    );

    // Then:
    final List<MetricsReporter> reporters = metricCollectors.getMetrics().reporters();
    assertThat(reporters, hasItem(mockReporter));
  }

  @Test
  public void shouldStartTheVersionCheckerAgentWithCorrectProperties() throws InterruptedException {
    // Given:
    final ArgumentCaptor<Properties> captor = ArgumentCaptor.forClass(Properties.class);
    final StandaloneExecutor standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        processingLogConfig,
        new KsqlConfig(ImmutableMap.of("confluent.support.metrics.enable", false)),
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        false,
        versionChecker,
        injectorFactory,
        new MetricCollectors(),
        rocksDBConfigSetterHandler
    );

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(versionChecker).start(eq(KsqlModuleType.SERVER), captor.capture());
    assertThat(captor.getValue().getProperty("confluent.support.metrics.enable"), equalTo("false"));
    standaloneExecutor.shutdown();
  }

  @Test
  public void shouldLoadQueryFile() {
    // Given:
    givenQueryFileContains("This statement");

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).parse("This statement");
  }


  @Test
  public void shouldNotThrowIfNullValueInKsqlConfig() {
    standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        processingLogConfig,
        new KsqlConfig(Collections.singletonMap("test", null)),
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        false,
        versionChecker,
        injectorFactory,
        new MetricCollectors(),
        rocksDBConfigSetterHandler
    );

    // When:
    standaloneExecutor.startAsync();
  }

  @Test
  public void shouldThrowIfCanNotLoadQueryFile() {
    // Given:
    givenFileDoesNotExist();

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> standaloneExecutor.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not read the query file"));
  }

  @Test
  public void shouldLoadUdfs() {
    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(udfLoader).load();
  }

  @Test
  public void shouldCreateProcessingLogTopic() {
    // When:
    standaloneExecutor.startAsync();

    // Then
    verify(kafkaTopicClient).createTopic(eq(PROCESSING_LOG_TOPIC_NAME), anyInt(), anyShort());
  }

  @Test
  public void shouldNotCreateProcessingLogTopicIfNotConfigured() {
    // Given:
    standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        new ProcessingLogConfig(ImmutableMap.of(
            ProcessingLogConfig.TOPIC_AUTO_CREATE, false,
            ProcessingLogConfig.TOPIC_NAME, PROCESSING_LOG_TOPIC_NAME
        )),
        ksqlConfig,
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        false,
        versionChecker,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
        new MetricCollectors(),
        rocksDBConfigSetterHandler
    );

    // When:
    standaloneExecutor.startAsync();

    // Then
    verify(kafkaTopicClient, times(0))
        .createTopic(eq(PROCESSING_LOG_TOPIC_NAME), anyInt(), anyShort());
  }

  @Test
  public void shouldFailOnDropStatement() {
    // Given:
    givenQueryFileParsesTo(
        PreparedStatement.of("DROP Test",
            new DropStream(SOME_NAME, false, false))
    );

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> standaloneExecutor.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unsupported statement. "
        + "Only the following statements are supporting in standalone mode:\n"
        + "CREAETE STREAM AS SELECT\n"
        + "CREATE STREAM\n"
        + "CREATE TABLE\n"
        + "CREATE TABLE AS SELECT\n"
        + "INSERT INTO\n"
        + "REGISTER TYPE\n"
        + "SET\n"
        + "UNSET"));
  }

  @Test
  public void shouldFailIfNoPersistentQueries() {
    // Given:
    givenExecutorWillFailOnNoQueries();

    givenQueryFileParsesTo(PreparedStatement.of("SET PROP",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> standaloneExecutor.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("The SQL file does not contain any persistent queries."));
  }

  @Test
  public void shouldRunCsStatement() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, false, JSON_PROPS, false));

    givenQueryFileParsesTo(cs);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext,
        ConfiguredStatement.of(cs, SessionConfig.of(ksqlConfig, emptyMap())));
  }

  @Test
  public void shouldRunCtStatement() {
    // Given:
    final PreparedStatement<CreateTable> ct = PreparedStatement.of("CT",
        new CreateTable(SOME_NAME, SOME_ELEMENTS, false, false, JSON_PROPS,
            false));

    givenQueryFileParsesTo(ct);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext,
        ConfiguredStatement.of(ct, SessionConfig.of(ksqlConfig, emptyMap())));
  }

  @Test
  public void shouldRunSetStatements() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET PROP",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, false, JSON_PROPS, false));

    givenQueryFileParsesTo(setProp, cs);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(
        serviceContext,
        ConfiguredStatement.of(cs, SessionConfig
                .of(ksqlConfig, ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        ));
  }

  @Test
  public void shouldSetPropertyOnlyOnCommandsFollowingTheSetStatement() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET PROP",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, false, JSON_PROPS, false));

    givenQueryFileParsesTo(cs, setProp);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext, ConfiguredStatement
        .of(cs, SessionConfig.of(ksqlConfig, ImmutableMap.of())));
  }

  @Test
  public void shouldRunUnSetStatements() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

    final PreparedStatement<UnsetProperty> unsetProp = PreparedStatement.of("UNSET",
        new UnsetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, false, JSON_PROPS, false));

    final ConfiguredStatement<?> configured = ConfiguredStatement
        .of(cs, SessionConfig.of(ksqlConfig, emptyMap()));

    givenQueryFileParsesTo(setProp, unsetProp, cs);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext, configured);
  }

  @Test
  public void shouldRunCsasStatements() {
    // Given:
    final PreparedStatement<?> csas = PreparedStatement.of("CSAS1",
        new CreateStreamAsSelect(SOME_NAME, query, false, false, CreateSourceAsProperties.none()));
    final ConfiguredStatement<?> configured = ConfiguredStatement
        .of(csas, SessionConfig.of(ksqlConfig, emptyMap()));
    givenQueryFileParsesTo(csas);

    when(sandBox.execute(sandBoxServiceContext, configured))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext, configured);
  }

  @Test
  public void shouldRunCtasStatements() {
    // Given:
    final PreparedStatement<?> ctas = PreparedStatement.of("CTAS",
        new CreateTableAsSelect(SOME_NAME, query, false, false, CreateSourceAsProperties.none()));
    final ConfiguredStatement<?> configured = ConfiguredStatement
        .of(ctas, SessionConfig.of(ksqlConfig, emptyMap()));

    givenQueryFileParsesTo(ctas);

    when(sandBox.execute(sandBoxServiceContext, configured))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext, configured);
  }

  @Test
  public void shouldRunInsertIntoStatements() {
    // Given:
    final PreparedStatement<?> insertInto = PreparedStatement.of("InsertInto",
        new InsertInto(SOME_NAME, query));
    final ConfiguredStatement<?> configured = ConfiguredStatement
        .of(insertInto, SessionConfig.of(ksqlConfig, emptyMap()));

    givenQueryFileParsesTo(insertInto);

    when(sandBox.execute(sandBoxServiceContext, configured))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(ksqlEngine).execute(serviceContext, configured);
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryDoesNotReturnQuery() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(sandBox.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of("well, this is unexpected."));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> standaloneExecutor.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not build the query"));
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryReturnsNonPersistentMetaData() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(sandBox.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of(nonPersistentQueryMd));

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> standaloneExecutor.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("Could not build the query"));
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowIfParseThrows() {
    // Given:
    when(ksqlEngine.parse(any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    standaloneExecutor.startAsync();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowIfExecuteThrows() {
    // Given:
    when(ksqlEngine.execute(any(), any(ConfiguredStatement.class)))
        .thenThrow(new RuntimeException("Boom!"));

    // When:
    standaloneExecutor.startAsync();
  }

  @Test
  public void shouldCloseEngineOnStop() {
    // When:
    standaloneExecutor.shutdown();

    // Then:
    verify(ksqlEngine).close();
  }

  @Test
  public void shouldCloseServiceContextOnStop() {
    // When:
    standaloneExecutor.shutdown();

    // Then:
    verify(serviceContext).close();
  }

  @Test
  public void shouldStartQueries() {
    // Given:
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(persistentQuery));

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(persistentQuery).start();
  }

  @Test
  public void shouldNotStartValidationPhaseQueries() {
    // Given:
    givenFileContainsAPersistentQuery();
    when(sandBox.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of(sandBoxQuery));

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(sandBoxQuery, never()).start();
  }

  @Test
  public void shouldOnlyPrepareNextStatementOncePreviousStatementHasBeenExecuted() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(
        ImmutableList.of(PARSED_STMT_0, PARSED_STMT_1));

    // When:
    standaloneExecutor.startAsync();

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine);
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_0);
    inOrder.verify(ksqlEngine).execute(serviceContext, CFG_STMT_0);
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_1);
    inOrder.verify(ksqlEngine).execute(serviceContext, CFG_STMT_1);
  }

  @Test
  public void shouldThrowOnCreateStatementWithNoElements() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, TableElements.of(), false, false, JSON_PROPS, false));

    givenQueryFileParsesTo(cs);

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> standaloneExecutor.startAsync()
    );

    // Then:
    assertThat(e.getMessage(), containsString("statement does not define the schema and the supplied format does not support schema inference"));
  }

  @Test
  public void shouldSupportSchemaInference() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, TableElements.of(), false, false, AVRO_PROPS, false));

    givenQueryFileParsesTo(cs);

    when(sandBoxSchemaInjector.inject(argThat(configured(equalTo(cs)))))
        .thenReturn((ConfiguredStatement) CFG_0_WITH_SCHEMA);

    when(schemaInjector.inject(argThat(configured(equalTo(cs)))))
        .thenReturn((ConfiguredStatement) CFG_1_WITH_SCHEMA);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(sandBox).execute(sandBoxServiceContext, CFG_0_WITH_SCHEMA);
    verify(ksqlEngine).execute(serviceContext, CFG_1_WITH_SCHEMA);
  }

  @Test
  public void shouldSupportTopicInference() {
    // Given:
    givenQueryFileParsesTo(PREPARED_CSAS);

    when(sandBoxTopicInjector.inject(argThat(configured(equalTo(PREPARED_CSAS)))))
        .thenReturn((ConfiguredStatement) CSAS_CFG_WITH_TOPIC);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(sandBox).execute(sandBoxServiceContext, CSAS_CFG_WITH_TOPIC);
  }

  @Test
  public void shouldConfigureRocksDBConfigSetter() {
    // Given:
    givenQueryFileParsesTo(PREPARED_CSAS);
    when(sandBoxTopicInjector.inject(argThat(configured(equalTo(PREPARED_CSAS)))))
        .thenReturn((ConfiguredStatement) CSAS_CFG_WITH_TOPIC);

    // When:
    standaloneExecutor.startAsync();

    // Then:
    verify(rocksDBConfigSetterHandler).accept(ksqlConfig);
  }

  private void givenExecutorWillFailOnNoQueries() {
    standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        true,
        versionChecker,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
        new MetricCollectors(),
        rocksDBConfigSetterHandler
    );
  }

  private void givenFileContainsAPersistentQuery() {
    givenQueryFileParsesTo(
        PreparedStatement.of("InsertInto", new InsertInto(SOME_NAME, query))
    );
  }

  private void givenQueryFileParsesTo(final PreparedStatement<?>... statements) {
    final List<ParsedStatement> parsedStmts = Arrays.stream(statements)
        .map(statement -> ParsedStatement
            .of(statement.getUnMaskedStatementText(), mock(SingleStatementContext.class)))
        .collect(Collectors.toList());

    when(ksqlEngine.parse(any())).thenReturn(parsedStmts);

    IntStream.range(0, parsedStmts.size()).forEach(idx -> {
      final ParsedStatement parsed = parsedStmts.get(idx);
      final PreparedStatement<?> prepared = statements[idx];
      when(sandBox.prepare(parsed)).thenReturn((PreparedStatement) prepared);
      when(ksqlEngine.prepare(parsed)).thenReturn((PreparedStatement) prepared);
    });
  }

  @SuppressWarnings("SameParameterValue")
  private void givenQueryFileContains(final String sql) {
    try {
      Files.write(queriesFile, sql.getBytes(StandardCharsets.UTF_8));
    } catch (final IOException e) {
      fail("invalid test: " + e.getMessage());
    }
  }

  private void givenFileDoesNotExist() {
    try {
      Files.delete(queriesFile);
    } catch (final IOException e) {
      fail("invalid test: " + e.getMessage());
    }
  }
}
