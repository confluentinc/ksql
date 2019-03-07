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

import static io.confluent.ksql.parser.ParserMatchers.preparedStatement;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type.SqlType;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.schema.inference.TopicInjector;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
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

  private static final List<TableElement> SOME_ELEMENTS = ImmutableList.of(
      new TableElement("bob", PrimitiveType.of(SqlType.STRING)));

  private static final QualifiedName SOME_NAME = QualifiedName.of("Bob");

  private static final ImmutableMap<String, Expression> JSON_PROPS = ImmutableMap
      .of("VALUE_FORMAT", new StringLiteral("json"));

  private static final String SOME_TOPIC = "some-topic";
  private static final ImmutableMap<String, Expression> AVRO_PROPS = ImmutableMap.of(
      "VALUE_FORMAT", new StringLiteral("avro"),
      "KAFKA_TOPIC", new StringLiteral(SOME_TOPIC));

  private static final CreateStream CREATE_STREAM = new CreateStream(
      SOME_NAME, SOME_ELEMENTS, true, JSON_PROPS);

  private static final CreateStreamAsSelect CREATE_STREAM_AS_SELECT = new CreateStreamAsSelect(
      QualifiedName.of("stream"),
      new Query(
          new QuerySpecification(
              new Select(true, ImmutableList.of(new AllColumns(new NodeLocation(0, 0)))),
              new Table(QualifiedName.of("sink")),
              true,
              new Table(QualifiedName.of("source")),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              OptionalInt.empty()
          ),
          OptionalInt.empty()
      ),
      false,
      ImmutableMap.of(),
      Optional.empty()
  );


  private final static ParsedStatement PARSED_STMT_0 = ParsedStatement
      .of("sql 0", mock(SingleStatementContext.class));

  private final static ParsedStatement PARSED_STMT_1 = ParsedStatement
      .of("sql 1", mock(SingleStatementContext.class));

  private static final ParsedStatement PARSED_CSAS = ParsedStatement
      .of("CSAS", mock(SingleStatementContext.class));

  private final static PreparedStatement<?> PREPARED_STMT_0 = PreparedStatement
      .of("sql 0", CREATE_STREAM);

  private final static PreparedStatement<?> PREPARED_STMT_1 = PreparedStatement
      .of("sql 1", CREATE_STREAM);

  private final static PreparedStatement<?> PREPARED_CSAS = PreparedStatement.
      of("CSAS", CREATE_STREAM_AS_SELECT);

  private final static PreparedStatement<CreateStream> STMT_0_WITH_SCHEMA = PreparedStatement
      .of("sql 0", new CreateStream(
          QualifiedName.of("CS 0"),
          SOME_ELEMENTS,
          true,
          Collections.emptyMap()
      ));

  private final static PreparedStatement<CreateStream> STMT_1_WITH_SCHEMA = PreparedStatement
      .of("sql 1", new CreateStream(
          QualifiedName.of("CS 1"),
          SOME_ELEMENTS,
          true,
          Collections.emptyMap()
      ));


  private final static PreparedStatement<CreateStreamAsSelect> CSAS_WITH_TOPIC = PreparedStatement
      .of("CSAS_TOPIC", CREATE_STREAM_AS_SELECT);

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
  private SchemaRegistryClient srClient;
  @Mock
  private Function<ServiceContext, SchemaInjector> schemaInjectorFactory;
  @Mock
  private SchemaInjector schemaInjector;
  @Mock
  private SchemaInjector sandBoxSchemaInjector;
  @Mock
  private Function<KsqlExecutionContext, TopicInjector> topicInjectorFactory;
  @Mock
  private TopicInjector topicInjector;
  @Mock
  private TopicInjector sandBoxTopicInjector;

  private Path queriesFile;
  private StandaloneExecutor standaloneExecutor;

  @Before
  public void before() throws Exception {
    queriesFile = Paths.get(TestUtils.tempFile().getPath());
    givenQueryFileContains("something");

    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);

    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0));

    when(ksqlEngine.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(ksqlEngine.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);
    when(ksqlEngine.prepare(PARSED_CSAS)).thenReturn((PreparedStatement) PREPARED_CSAS);

    when(ksqlEngine.execute(any(), any(), any())).thenReturn(ExecuteResult.of(persistentQuery));

    when(ksqlEngine.createSandbox()).thenReturn(sandBox);

    when(sandBox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandBox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);
    when(sandBox.prepare(PARSED_CSAS)).thenReturn((PreparedStatement) PREPARED_CSAS);

    when(sandBox.execute(any(), any(), any())).thenReturn(ExecuteResult.of("success"));
    when(sandBox.execute(eq(CSAS_WITH_TOPIC), any(), any()))
        .thenReturn(ExecuteResult.of(persistentQuery));

    when(schemaInjectorFactory.apply(any())).thenReturn(sandBoxSchemaInjector);
    when(schemaInjectorFactory.apply(serviceContext)).thenReturn(schemaInjector);
    when(sandBoxSchemaInjector.forStatement(any())).thenAnswer(inv -> inv.getArgument(0));
    when(schemaInjector.forStatement(any())).thenAnswer(inv -> inv.getArgument(0));

    when(topicInjectorFactory.apply(any())).thenReturn(sandBoxTopicInjector);
    when(topicInjectorFactory.apply(ksqlEngine)).thenReturn(topicInjector);
    when(sandBoxTopicInjector.forStatement(any(), any(), any()))
        .thenAnswer(inv -> inv.getArgument(0));
    when(topicInjector.forStatement(any(), any(), any())).thenAnswer(inv -> inv.getArgument(0));

    standaloneExecutor = new StandaloneExecutor(
        serviceContext,
        processingLogConfig,
        ksqlConfig,
        ksqlEngine,
        queriesFile.toString(),
        udfLoader,
        false,
        versionChecker,
        schemaInjectorFactory,
        topicInjectorFactory);
  }

  @Test
  public void shouldStartTheVersionCheckerAgent() {
    // When:
    standaloneExecutor.start();

    verify(versionChecker).start(eq(KsqlModuleType.SERVER), any());
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
        schemaInjectorFactory,
        topicInjectorFactory
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
        new SetProperty(Optional.empty(), "name", "value")));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The SQL file did not contain any queries");

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
    verify(ksqlEngine).execute(cs, ksqlConfig, emptyMap());
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
    verify(ksqlEngine).execute(ct, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldRunSetStatements() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET PROP",
        new SetProperty(Optional.empty(), "name", "value"));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, JSON_PROPS));

    givenQueryFileParsesTo(setProp, cs);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(eq(cs), any(), eq(ImmutableMap.of("name", "value")));
  }

  @Test
  public void shouldRunUnSetStatements() {
    // Given:
    final PreparedStatement<SetProperty> setProp = PreparedStatement.of("SET",
        new SetProperty(Optional.empty(), "name", "value"));

    final PreparedStatement<UnsetProperty> unsetProp = PreparedStatement.of("UNSET",
        new UnsetProperty(Optional.empty(), "name"));

    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, SOME_ELEMENTS, false, JSON_PROPS));

    givenQueryFileParsesTo(setProp, unsetProp, cs);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(eq(cs), any(), eq(emptyMap()));
  }

  @Test
  public void shouldRunCsasStatements() {
    // Given:
    final PreparedStatement<?> csas = PreparedStatement.of("CSAS1",
        new CreateStreamAsSelect(SOME_NAME, query, false, emptyMap(), Optional.empty()));

    givenQueryFileParsesTo(csas);

    when(sandBox.execute(eq(csas), any(), any()))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(csas, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldRunCtasStatements() {
    // Given:
    final PreparedStatement<?> ctas = PreparedStatement.of("CTAS",
        new CreateTableAsSelect(SOME_NAME, query, false, emptyMap()));

    givenQueryFileParsesTo(ctas);

    when(sandBox.execute(eq(ctas), any(), any()))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(ctas, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldRunInsertIntoStatements() {
    // Given:
    final PreparedStatement<?> insertInto = PreparedStatement.of("InsertInto",
        new InsertInto(SOME_NAME, query, Optional.empty()));

    givenQueryFileParsesTo(insertInto);

    when(sandBox.execute(eq(insertInto), any(), any()))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(ksqlEngine).execute(insertInto, ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryDoesNotReturnQuery() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(sandBox.execute(any(), any(), any()))
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

    when(sandBox.execute(any(), any(), any()))
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
    when(ksqlEngine.execute(any(), any(), any())).thenThrow(new RuntimeException("Boom!"));

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
    when(sandBox.execute(any(), any(), any())).thenReturn(ExecuteResult.of(sandBoxQuery));

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
    inOrder.verify(ksqlEngine).execute(eq(PREPARED_STMT_0), any(), any());
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_1);
    inOrder.verify(ksqlEngine).execute(eq(PREPARED_STMT_1), any(), any());
  }

  @Test
  public void shouldThrowOnCreateStatementWithNoElements() {
    // Given:
    final PreparedStatement<CreateStream> cs = PreparedStatement.of("CS",
        new CreateStream(SOME_NAME, emptyList(), false, JSON_PROPS));

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
        new CreateStream(SOME_NAME, emptyList(), false, AVRO_PROPS));

    givenQueryFileParsesTo(cs);

    when(sandBoxSchemaInjector.forStatement(cs))
        .thenReturn(STMT_0_WITH_SCHEMA);

    when(schemaInjector.forStatement(cs))
        .thenReturn(STMT_1_WITH_SCHEMA);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(sandBox).execute(eq(STMT_0_WITH_SCHEMA), any(), any());
    verify(ksqlEngine).execute(eq(STMT_1_WITH_SCHEMA), any(), any());
  }

  @Test
  public void shouldSupportTopicInference() {
    // Given:
    givenQueryFileParsesTo(PREPARED_CSAS);

    when(sandBoxTopicInjector.forStatement(eq(PREPARED_CSAS), any(), any()))
        .thenReturn((PreparedStatement) CSAS_WITH_TOPIC);

    // When:
    standaloneExecutor.start();

    // Then:
    verify(sandBox).execute(eq(CSAS_WITH_TOPIC), any(), any());
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
        schemaInjectorFactory,
        topicInjectorFactory
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
