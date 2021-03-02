/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.tools.migrations.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import com.github.rvesse.airline.SingleCommand;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.impl.RowImpl;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import io.vertx.core.json.JsonArray;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)

public class ApplyMigrationCommandTest {

  private static final SingleCommand<ApplyMigrationCommand> PARSER =
      SingleCommand.singleCommand(ApplyMigrationCommand.class);

  private static final String MIGRATIONS_TABLE = "migrations_table";
  private static final String MIGRATIONS_STREAM = "migrations_stream";
  private static final String NAME = "FOO";
  private static final String COMMAND = "CREATE STREAM FOO (A STRING) WITH (KAFKA_TOPIC='FOO', PARTITIONS=1, VALUE_FORMAT='DELIMITED');";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Mock
  private MigrationConfig config;
  @Mock
  private Client ksqlClient;
  @Mock
  private BatchedQueryResult versionQueryResult;
  @Mock
  private BatchedQueryResult infoQueryResult;
  @Mock
  private ExecuteStatementResult statementResult;
  @Mock
  private SourceDescription sourceDescription;
  @Mock
  private CompletableFuture<Void> insertResult;
  @Mock
  private CompletableFuture<ExecuteStatementResult> statementResultCf;
  @Mock
  private CompletableFuture<SourceDescription> sourceDescriptionCf;

  private String migrationsDir;
  private ApplyMigrationCommand command;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME)).thenReturn(MIGRATIONS_TABLE);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME)).thenReturn(MIGRATIONS_STREAM);

    when(ksqlClient.insertInto(any(), any())).thenReturn(insertResult);
    when(ksqlClient.executeStatement(any())).thenReturn(statementResultCf);
    when(ksqlClient.executeQuery("SELECT VERSION FROM " + MIGRATIONS_TABLE + " WHERE version_key = 'CURRENT';"))
        .thenReturn(versionQueryResult);
    when(ksqlClient.executeQuery("SELECT checksum, previous, state FROM " + MIGRATIONS_TABLE + " WHERE version_key = '1';"))
        .thenReturn(infoQueryResult);
    when(ksqlClient.describeSource(MIGRATIONS_STREAM)).thenReturn(sourceDescriptionCf);
    when(ksqlClient.describeSource(MIGRATIONS_TABLE)).thenReturn(sourceDescriptionCf);
    when(sourceDescriptionCf.get()).thenReturn(sourceDescription);
    when(statementResultCf.get()).thenReturn(statementResult);

    migrationsDir = folder.getRoot().getPath();
  }

  @Test
  public void shouldApplyFirstMigration() throws Exception {
    // Given:
    command = PARSER.parse("-n");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(0));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 1, "<none>", MigrationState.MIGRATED);
    inOrder.verify(ksqlClient).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldApplySecondMigration() throws Exception {
    // Given:
    command = PARSER.parse("-n");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    createMigrationFile(3, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of(createVersionRow("1")));
    when(infoQueryResult.get()).thenReturn(ImmutableList.of(createInfoRow(1, NAME, MigrationState.MIGRATED)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(0));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 3, "1", MigrationState.MIGRATED);
    inOrder.verify(ksqlClient).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldApplyMultipleMigrations() throws Exception {
    // Given:
    command = PARSER.parse("-a");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    createMigrationFile(2, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());
    when(infoQueryResult.get()).thenReturn(ImmutableList.of(createInfoRow(1, NAME, MigrationState.MIGRATED)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(0));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 1, "<none>", MigrationState.MIGRATED);
    verifyMigratedVersion(inOrder, 2, "1", MigrationState.MIGRATED);
    inOrder.verify(ksqlClient).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldApplyUntilVersion() throws Exception {
    // Given:
    command = PARSER.parse("-u", "2");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    createMigrationFile(2, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());
    when(infoQueryResult.get()).thenReturn(ImmutableList.of(createInfoRow(1, NAME, MigrationState.MIGRATED)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(0));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 1, "<none>", MigrationState.MIGRATED);
    verifyMigratedVersion(inOrder, 2, "1", MigrationState.MIGRATED);
    inOrder.verify(ksqlClient).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldNotApplyMigrationIfPreviousNotFinished() throws Exception {
    // Given:
    command = PARSER.parse("-a");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    createMigrationFile(2, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());
    when(infoQueryResult.get()).thenReturn(ImmutableList.of(createInfoRow(1, NAME, MigrationState.RUNNING)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(1));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 1, "<none>", MigrationState.MIGRATED);
    inOrder.verify(ksqlClient).close();
    Mockito.verify(ksqlClient, Mockito.times(1)).executeStatement(COMMAND);
  }

  @Test
  public void shouldLogErrorStateIfMigrationFails() throws Exception {
    // Given:
    command = PARSER.parse("-n");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());
    when(statementResultCf.get()).thenThrow(new InterruptedException());

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(1));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 1, "<none>", MigrationState.ERROR);
    inOrder.verify(ksqlClient).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldSkipApplyIfValidateFails() throws Exception {
    // Given:
    command = PARSER.parse("-n");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    createMigrationFile(1, "anotherone", migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of(createVersionRow("1")));
    when(infoQueryResult.get()).thenReturn(ImmutableList.of(createInfoRow(1, NAME, MigrationState.MIGRATED)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(1));
    Mockito.verify(ksqlClient, Mockito.times(3)).executeQuery(any());
    Mockito.verify(ksqlClient, Mockito.times(0)).executeStatement(any());
    Mockito.verify(ksqlClient, Mockito.times(0)).insertInto(any(), any());
  }

  @Test
  public void shouldNotFailIfFileDoesntFitFormat() throws Exception {
    // Given:
    command = PARSER.parse("-n");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());

    // extra file that does not match expected format
    assertThat(new File(migrationsDir + "/foo.sql").createNewFile(), is(true));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(0));
    final InOrder inOrder = inOrder(ksqlClient);
    verifyMigratedVersion(inOrder, 1, "<none>", MigrationState.MIGRATED);
    inOrder.verify(ksqlClient).close();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldFailIfMetadataNotInitialized() throws Exception {
    // Given:
    command = PARSER.parse("-n");
    createMigrationFile(1, NAME, migrationsDir, COMMAND);
    when(versionQueryResult.get()).thenReturn(ImmutableList.of());

    when(sourceDescriptionCf.get())
        .thenThrow(new ExecutionException("Source not found", new RuntimeException()));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir, Clock.fixed(
        Instant.ofEpochMilli(1000), ZoneId.systemDefault()));

    // Then:
    assertThat(result, is(1));
    Mockito.verify(ksqlClient, Mockito.times(0)).executeStatement(any());
    Mockito.verify(ksqlClient, Mockito.times(0)).insertInto(any(), any());
  }

  private void createMigrationFile(
      final int version,
      final String name,
      final String migrationsDir,
      final String content
  ) throws IOException {
    final String filePath = getMigrationFilePath(version, name, migrationsDir);
    assertThat(new File(filePath).createNewFile(), is(true));
    PrintWriter out = new PrintWriter(filePath, Charset.defaultCharset().name());
    out.println(content);
    out.close();
  }

  private String getMigrationFilePath(
      final int version,
      final String name,
      final String migrationsDir
  ) {
    return migrationsDir
        + String.format("/V00000%d__%s.sql", version, name.replace(' ', '_'));
  }

  private KsqlObject createKsqlObject(
      final String versionKey,
      final int version,
      final String name,
      final MigrationState state,
      final String startOn,
      final String completedOn,
      final String previous
  ) {
    final List<String> KEYS = ImmutableList.of(
        "VERSION_KEY", "VERSION", "NAME", "STATE",
        "CHECKSUM", "STARTED_ON", "COMPLETED_ON", "PREVIOUS"
    );

    final List<String> values = ImmutableList.of(
        versionKey,
        Integer.toString(version),
        name,
        state.toString(),
        MigrationsDirectoryUtil.computeHashForFile(getMigrationFilePath(version, name, migrationsDir)),
        startOn,
        completedOn,
        previous
    );

    return KsqlObject.fromArray(KEYS, new KsqlArray(values));
  }

  private Row createVersionRow(final String version) {
    return new RowImpl(
        ImmutableList.of("VERSION"),
        RowUtil.columnTypesFromStrings(ImmutableList.of("STRING")),
        new JsonArray(ImmutableList.of(version)),
        ImmutableMap.of("VERSION", 1)
    );
  }

  private Row createInfoRow(final int version, final String name, final MigrationState state) {
    final String checksum = MigrationsDirectoryUtil.computeHashForFile(getMigrationFilePath(version, name, migrationsDir));
    final String previous = version == 1 ? MetadataUtil.NONE_VERSION : Integer.toString(version - 1);
    return new RowImpl(
        ImmutableList.of("CHECKSUM", "PREVIOUS", "STATE"),
        RowUtil.columnTypesFromStrings(ImmutableList.of("STRING", "STRING", "STRING")),
        new JsonArray(ImmutableList.of(checksum, previous, state.toString())),
        ImmutableMap.of("CHECKSUM", 1, "PREVIOUS", 2, "STATE", 3)
    );
  }

  private void verifyMigratedVersion(final InOrder inOrder, final int version, final String previous, final MigrationState finalState) {
    inOrder.verify(ksqlClient).insertInto(
        MIGRATIONS_STREAM,
        createKsqlObject(MetadataUtil.CURRENT_VERSION_KEY, version, NAME, MigrationState.RUNNING, "1000", "", previous)
    );
    inOrder.verify(ksqlClient).insertInto(
        MIGRATIONS_STREAM,
        createKsqlObject(Integer.toString(version), version, NAME, MigrationState.RUNNING, "1000", "", previous)
    );
    inOrder.verify(ksqlClient).executeStatement(COMMAND);
    inOrder.verify(ksqlClient).insertInto(
        MIGRATIONS_STREAM,
        createKsqlObject(MetadataUtil.CURRENT_VERSION_KEY, version, NAME, finalState, "1000", "1000", previous)
    );
    inOrder.verify(ksqlClient).insertInto(
        MIGRATIONS_STREAM,
        createKsqlObject(Integer.toString(version), version, NAME, finalState, "1000", "1000", previous)
    );
  }
}
