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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
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
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
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
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
  private static final KsqlConfig ksqlConfig = new KsqlConfig(emptyMap());

  private static final TableElements SOME_ELEMENTS = TableElements.of(
      new TableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING)));

  private static final QualifiedName SOME_NAME = QualifiedName.of("Bob");
  private static final String SOME_TOPIC = "some-topic";

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
      SOME_NAME, SOME_ELEMENTS, true, JSON_PROPS);

  private static final CreateStreamAsSelect CREATE_STREAM_AS_SELECT = new CreateStreamAsSelect(
      QualifiedName.of("stream"),
      new Query(
          Optional.empty(),
          new Select(ImmutableList.of(new AllColumns(Optional.empty()))),
          new Table(QualifiedName.of("sink")),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          OptionalInt.empty()
      ),
      false,
      CreateSourceAsProperties.none(),
      Optional.empty()
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

  private final static ConfiguredStatement<?> CFG_STMT_0 = ConfiguredStatement.of(
      PREPARED_STMT_0, emptyMap(), ksqlConfig);

  private final static ConfiguredStatement<?> CFG_STMT_1 = ConfiguredStatement.of(
      PREPARED_STMT_1, emptyMap(), ksqlConfig);

  private final static PreparedStatement<CreateStream> STMT_0_WITH_SCHEMA = PreparedStatement
      .of("sql 0", new CreateStream(
          QualifiedName.of("CS 0"),
          SOME_ELEMENTS,
          true,
          JSON_PROPS
      ));

  private final static ConfiguredStatement<?> CFG_0_WITH_SCHEMA = ConfiguredStatement.of(
      STMT_0_WITH_SCHEMA, emptyMap(), ksqlConfig);

  private final static PreparedStatement<CreateStream> STMT_1_WITH_SCHEMA = PreparedStatement
      .of("sql 1", new CreateStream(
          QualifiedName.of("CS 1"),
          SOME_ELEMENTS,
          true,
          JSON_PROPS
      ));

  private final static ConfiguredStatement<?> CFG_1_WITH_SCHEMA = ConfiguredStatement.of(
      STMT_1_WITH_SCHEMA, emptyMap(), ksqlConfig);


  private final static PreparedStatement<CreateStreamAsSelect> CSAS_WITH_TOPIC = PreparedStatement
      .of("CSAS_TOPIC", CREATE_STREAM_AS_SELECT);

  private final static ConfiguredStatement<CreateStreamAsSelect> CSAS_CFG_WITH_TOPIC =
      ConfiguredStatement.of(CSAS_WITH_TOPIC, emptyMap(), ksqlConfig);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Query query;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlExecutionContext sandBox;
  @Mock
  private UdfLoader udfLoader;
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

    when(ksqlEngine.execute(any())).thenReturn(ExecuteResult.of(persistentQuery));

    when(ksqlEngine.createSandbox(any())).thenReturn(sandBox);

    when(sandBox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandBox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(sandBox.execute(any())).thenReturn(ExecuteResult.of("success"));
    when(sandBox.execute(CSAS_CFG_WITH_TOPIC))
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
        injectorFactory
    );
  }

  @Test
  public void shouldStartTheVersionCheckerAgent() {
    // When:
    standaloneExecutor.start();

    verify(versionChecker).start(eq(KsqlModuleType.SERVER), any());
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
        injectorFactory);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(versionChecker).start(eq(KsqlModuleType.SERVER), captor.capture());
    assertThat(captor.getValue().getProperty("confluent.support.metrics.enable"), equalTo("false"));
    standaloneExecutor.stop();
    standaloneExecutor.join();
  }

  @Test
  public void shouldLoadQueryFile() {
    // Given:
    givenQueryFileContains("This statement");

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).parse("This statement");
  }

  @Test
  public void shouldThrowIfCanNotLoadQueryFile() {
    // Given:
    givenFileDoesNotExist();

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not read the query file");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldLoadUdfs() {
    // When:
    standaloneExecutor.start();

    // Then:
    verify(udfLoader).load();
  }

  @Test
  public void shouldCreateProcessingLogTopic() {
    // When:
    standaloneExecutor.start();

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
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector)
    );

    // When:
    standaloneExecutor.start();

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

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Unsupported statement. "
        + "Only the following statements are supporting in standalone mode:\n"
        + "CREAETE STREAM AS SELECT\n"
        + "CREATE STREAM\n"
        + "CREATE TABLE\n"
        + "CREATE TABLE AS SELECT\n"
        + "INSERT INTO\n"
        + "SET\n"
        + "UNSET");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldFailIfNoPersistentQueries() {
    // Given:
    givenExecutorWillFailOnNoQueries();

    givenQueryFileParsesTo(PreparedStatement.of("SET PROP",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
      "The SQL file does not contain any persistent queries."
    );

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldRunCsStatement() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, JSON_PROPS));

    givenQueryFileParsesTo(cs);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(ConfiguredStatement.of(cs, emptyMap(), ksqlConfig));
  }

  @Test
  public void shouldRunCtStatement() {
    // Given:
    final PreparedStatement<CreateTable> ct = PreparedStatement.of("CT",
        new CreateTable(SOME_NAME, SOME_ELEMENTS, false, JSON_PROPS));

    givenQueryFileParsesTo(ct);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(ConfiguredStatement.of(ct, emptyMap(), ksqlConfig));
  }

  @Test
  public void shouldRunSetStatements() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET PROP",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, JSON_PROPS));

    givenQueryFileParsesTo(setProp, cs);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(
        ConfiguredStatement.of(
            cs,
            ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
            ksqlConfig));
  }

  @Test
  public void shouldRunUnSetStatements() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));

    final PreparedStatement<UnsetProperty> unsetProp = PreparedStatement.of("UNSET",
        new UnsetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, JSON_PROPS));

    final ConfiguredStatement<?> configured = ConfiguredStatement.of(cs, emptyMap(), ksqlConfig);

    givenQueryFileParsesTo(setProp, unsetProp, cs);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(configured);
  }

  @Test
  public void shouldRunCsasStatements() {
    // Given:
    final PreparedStatement<?> csas = PreparedStatement.of("CSAS1",
        new CreateStreamAsSelect(SOME_NAME, query, false, CreateSourceAsProperties.none(), Optional.empty()));
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(csas, emptyMap(), ksqlConfig);
    givenQueryFileParsesTo(csas);

    when(sandBox.execute(configured))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(configured);
  }

  @Test
  public void shouldRunCtasStatements() {
    // Given:
    final PreparedStatement<?> ctas = PreparedStatement.of("CTAS",
        new CreateTableAsSelect(SOME_NAME, query, false, CreateSourceAsProperties.none()));
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(ctas, emptyMap(), ksqlConfig);

    givenQueryFileParsesTo(ctas);

    when(sandBox.execute(configured))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(configured);
  }

  @Test
  public void shouldRunInsertIntoStatements() {
    // Given:
    final PreparedStatement<?> insertInto = PreparedStatement.of("InsertInto",
        new InsertInto(SOME_NAME, query, Optional.empty()));
    final ConfiguredStatement<?> configured = ConfiguredStatement.of(insertInto, emptyMap(), ksqlConfig);

    givenQueryFileParsesTo(insertInto);

    when(sandBox.execute(configured))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(configured);
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryDoesNotReturnQuery() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(sandBox.execute(any()))
        .thenReturn(ExecuteResult.of("well, this is unexpected."));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not build the query");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryReturnsNonPersistentMetaData() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(sandBox.execute(any()))
        .thenReturn(ExecuteResult.of(nonPersistentQueryMd));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not build the query");

    // When:
    standaloneExecutor.start();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowIfParseThrows() {
    // Given:
    when(ksqlEngine.parse(any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    standaloneExecutor.start();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowIfExecuteThrows() {
    // Given:
    when(ksqlEngine.execute(any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldCloseEngineOnStop() {
    // When:
    standaloneExecutor.stop();

    // Then:
    verify(ksqlEngine).close();
  }

  @Test
  public void shouldCloseServiceContextOnStop() {
    // When:
    standaloneExecutor.stop();

    // Then:
    verify(serviceContext).close();
  }

  @Test
  public void shouldStartQueries() {
    // Given:
    when(ksqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(persistentQuery).start();
  }

  @Test
  public void shouldNotStartValidationPhaseQueries() {
    // Given:
    givenFileContainsAPersistentQuery();
    when(sandBox.execute(any())).thenReturn(ExecuteResult.of(sandBoxQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(sandBoxQuery, never()).start();
  }

  @Test
  public void shouldOnlyPrepareNextStatementOncePreviousStatementHasBeenExecuted() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(
        ImmutableList.of(PARSED_STMT_0, PARSED_STMT_1));

    // When:
    standaloneExecutor.start();

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine);
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_0);
    inOrder.verify(ksqlEngine).execute(CFG_STMT_0);
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_1);
    inOrder.verify(ksqlEngine).execute(CFG_STMT_1);
  }

  @Test
  public void shouldThrowOnCreateStatementWithNoElements() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, TableElements.of(), false, JSON_PROPS));

    givenQueryFileParsesTo(cs);

    // Then:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage(
        "statement does not define the schema and the supplied format does not support schema inference");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldSupportSchemaInference() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, TableElements.of(), false, AVRO_PROPS));

    givenQueryFileParsesTo(cs);

    when(sandBoxSchemaInjector.inject(argThat(configured(equalTo(cs)))))
        .thenReturn((ConfiguredStatement) CFG_0_WITH_SCHEMA);

    when(schemaInjector.inject(argThat(configured(equalTo(cs)))))
        .thenReturn((ConfiguredStatement) CFG_1_WITH_SCHEMA);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(sandBox).execute(CFG_0_WITH_SCHEMA);
    verify(ksqlEngine).execute(CFG_1_WITH_SCHEMA);
  }

  @Test
  public void shouldSupportTopicInference() {
    // Given:
    givenQueryFileParsesTo(PREPARED_CSAS);

    when(sandBoxTopicInjector.inject(argThat(configured(equalTo(PREPARED_CSAS)))))
        .thenReturn((ConfiguredStatement) CSAS_CFG_WITH_TOPIC);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(sandBox).execute(CSAS_CFG_WITH_TOPIC);
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
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector)
    );
  }

  private void givenFileContainsAPersistentQuery() {
    givenQueryFileParsesTo(
        PreparedStatement.of("InsertInto", new InsertInto(SOME_NAME, query, Optional.empty()))
    );
  }

  private void givenQueryFileParsesTo(final PreparedStatement<?>... statements) {
    final List<ParsedStatement> parsedStmts = Arrays.stream(statements)
        .map(statement -> ParsedStatement
            .of(statement.getStatementText(), mock(SingleStatementContext.class)))
        .collect(Collectors.toList());

    when(ksqlEngine.parse(any())).thenReturn(parsedStmts);

    IntStream.range(0, parsedStmts.size()).forEach(idx -> {
      final ParsedStatement parsed = parsedStmts.get(idx);
      final PreparedStatement<?> prepared = statements[idx];
      when(sandBox.prepare(parsed)).thenReturn((PreparedStatement)prepared);
      when(ksqlEngine.prepare(parsed)).thenReturn((PreparedStatement)prepared);
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
