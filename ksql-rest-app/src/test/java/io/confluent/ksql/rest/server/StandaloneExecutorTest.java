/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.version.metrics.VersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StandaloneExecutorTest {

  private static final KsqlConfig ksqlConfig = new KsqlConfig(emptyMap());
  private static final QualifiedName SOME_NAME = QualifiedName.of("Test");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Query query;
  @Mock
  private KsqlEngine engine;
  @Mock
  private UdfLoader udfLoader;
  @Mock
  private PersistentQueryMetadata queryMd;
  @Mock
  private QueryMetadata nonPeristentQueryMd;
  @Mock
  private VersionCheckerAgent versionCheckerAgent;
  @Mock
  private ServiceContext serviceContext;

  private Path queriesFile;
  private StandaloneExecutor standaloneExecutor;

  @Before
  public void before() throws IOException {
    queriesFile = Paths.get(TestUtils.tempFile().getPath());

    when(engine.execute(any(), any(), any())).thenReturn(ImmutableList.of(queryMd));

    standaloneExecutor = new StandaloneExecutor(
        serviceContext, ksqlConfig, engine, queriesFile.toString(), udfLoader,
        false, versionCheckerAgent);
  }

  @Test
  public void shouldStartTheVersionCheckerAgent() {
    // When:
    standaloneExecutor.start();

    verify(versionCheckerAgent).start(eq(KsqlModuleType.SERVER), any());
  }

  @Test
  public void shouldLoadQueryFile() {
    // Given:
    givenQueryFileContains("This statement");

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).parseStatements("This statement");
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
  public void shouldFailOnDropStatement() {
    // Given:
    when(engine.parseStatements(any())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("DROP",
            new DropStream(SOME_NAME, false, false))
    ));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Ignoring statements: DROP\n"
        + "Only DDL (CREATE STREAM/TABLE, DROP STREAM/TABLE, SET, UNSET) "
        + "and DML(CSAS, CTAS and INSERT INTO) statements can run in standalone mode.");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldFailIfNoPersistentQueries() {
    // Given:
    givenExecutorWillFailOnNoQueries();

    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("Transient query", query)
    ));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("The SQL file did not contain any queries");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldRunCsStatement() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("CS",
            new CreateStream(SOME_NAME, emptyList(), false, emptyMap()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute("CS", ksqlConfig, emptyMap());
  }

  @Test
  public void shouldRunCtStatement() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("CT",
            new CreateTable(SOME_NAME, emptyList(), false, emptyMap()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute("CT", ksqlConfig, emptyMap());
  }

  @Test
  public void shouldRunSetStatements() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("CS",
            new SetProperty(Optional.empty(), "name", "value")),
        new PreparedStatement<>("CS",
            new CreateStream(SOME_NAME, emptyList(), false, emptyMap()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute(any(), any(), eq(ImmutableMap.of("name", "value")));
  }

  @Test
  public void shouldRunUnSetStatements() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("SET",
            new SetProperty(Optional.empty(), "name", "value")),
        new PreparedStatement<>("UNSET",
            new UnsetProperty(Optional.empty(), "name")),
        new PreparedStatement<>("CS",
            new CreateStream(SOME_NAME, emptyList(), false, emptyMap()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute(any(), any(), eq(emptyMap()));
  }

  @Test
  public void shouldRunCsasStatements() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("CSAS1",
            new CreateStreamAsSelect(SOME_NAME, query, false, emptyMap(), Optional.empty()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute("CSAS1", ksqlConfig, emptyMap());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCtasStatements() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("CTAS",
            new CreateTableAsSelect(SOME_NAME, query, false, emptyMap()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute("CTAS", ksqlConfig, emptyMap());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunInsertIntoStatements() {
    // Given:
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("InsertInto",
            new InsertInto(SOME_NAME, query, Optional.empty()))
    ));

    // When:
    standaloneExecutor.start();

    // Then:
    verify(engine).execute("InsertInto", ksqlConfig, emptyMap());
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryDoesNotReturnQuery() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(engine.execute(any(), any(), any())).thenReturn(emptyList());

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not build the query");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryReturnsMultiple() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(engine.execute(any(), any(), any())).thenReturn(ImmutableList.of(queryMd, queryMd));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not build the query");

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldThrowIfExecutingPersistentQueryReturnsNonPersistentMetaData() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(engine.execute(any(), any(), any())).thenReturn(ImmutableList.of(nonPeristentQueryMd));

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not build the query");

    // When:
    standaloneExecutor.start();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowIfParseThrows() {
    // Given:
    when(engine.parseStatements(any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    standaloneExecutor.start();
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowIfExecuteThrows() {
    // Given:
    givenFileContainsAPersistentQuery();

    when(engine.execute(any(), any(), any())).thenThrow(new RuntimeException("Boom!"));

    // When:
    standaloneExecutor.start();
  }

  @Test
  public void shouldCloseEngineOnStop() {
    // When:
    standaloneExecutor.stop();

    // Then:
    verify(engine).close();
  }

  @Test
  public void shouldCloseServiceContextOnStop() {
    // When:
    standaloneExecutor.stop();

    // Then:
    verify(serviceContext).close();
  }

  private void givenExecutorWillFailOnNoQueries() {
    standaloneExecutor = new StandaloneExecutor(
        serviceContext, ksqlConfig, engine, queriesFile.toString(), udfLoader, true, versionCheckerAgent);
  }

  private void givenFileContainsAPersistentQuery() {
    when(engine.parseStatements(anyString())).thenReturn(ImmutableList.of(
        new PreparedStatement<>("InsertInto",
            new InsertInto(SOME_NAME, query, Optional.empty()))
    ));
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