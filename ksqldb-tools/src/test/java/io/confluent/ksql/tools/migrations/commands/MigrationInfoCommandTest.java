/*
 * Copyright 2020 Confluent Inc.
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

import static io.confluent.ksql.tools.migrations.util.MetadataUtil.CURRENT_VERSION_KEY;
import static io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState.ERROR;
import static io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState.MIGRATED;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getFilePrefixForVersion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.rvesse.airline.SingleCommand;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MigrationInfoCommandTest {

  private static final SingleCommand<MigrationInfoCommand> PARSER =
      SingleCommand.singleCommand(MigrationInfoCommand.class);

  private static final String MIGRATIONS_STREAM = "migrations_stream";
  private static final String MIGRATIONS_TABLE = "migrations_table";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Mock
  private MigrationConfig config;
  @Mock
  private Client ksqlClient;
  @Mock
  private CompletableFuture<SourceDescription> sourceDescriptionCf;
  @Mock
  private CompletableFuture<ServerInfo> serverInfoCf;
  @Mock
  private SourceDescription sourceDescription;
  @Mock
  private ServerInfo serverInfo;

  @Mock
  private AppenderSkeleton logAppender;
  @Captor
  private ArgumentCaptor<LoggingEvent> logCaptor;

  private String migrationsDir;
  private MigrationInfoCommand command;

  @Before
  public void setUp() throws Exception {
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME)).thenReturn(MIGRATIONS_STREAM);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME)).thenReturn(MIGRATIONS_TABLE);
    when(ksqlClient.describeSource(MIGRATIONS_STREAM)).thenReturn(sourceDescriptionCf);
    when(ksqlClient.describeSource(MIGRATIONS_TABLE)).thenReturn(sourceDescriptionCf);
    when(ksqlClient.serverInfo()).thenReturn(serverInfoCf);
    when(sourceDescriptionCf.get()).thenReturn(sourceDescription);
    when(serverInfoCf.get()).thenReturn(serverInfo);
    when(serverInfo.getServerVersion()).thenReturn("v0.14.0");

    migrationsDir = folder.getRoot().getPath();
    command = PARSER.parse();

    Logger.getRootLogger().addAppender(logAppender);
  }

  @After
  public void tearDown() {
    Logger.getRootLogger().removeAppender(logAppender);
  }

  @Test
  public void shouldPrintInfo() throws Exception {
    // Given:
    givenMigrations(
        ImmutableList.of("1", "3"),
        ImmutableList.of(MIGRATED, ERROR),
        ImmutableList.of("N/A", "error reason"),
        ImmutableList.of("4"));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verify(logAppender, atLeastOnce()).doAppend(logCaptor.capture());
    final List<String> logMessages = logCaptor.getAllValues().stream()
        .map(LoggingEvent::getRenderedMessage)
        .collect(Collectors.toList());
    assertThat(logMessages, hasItem(containsString("Current migration version: 3")));
    assertThat(logMessages, hasItem(containsString(
        " Version | Name        | State    | Previous Version | Started On | Completed On | Error Reason \n" +
            "------------------------------------------------------------------------------------------------\n" +
            " 1       | some_name_1 | MIGRATED | <none>           | N/A        | N/A          | N/A          \n" +
            " 3       | some_name_3 | ERROR    | 1                | N/A        | N/A          | error reason \n" +
            " 4       | some name 4 | PENDING  | N/A              | N/A        | N/A          | N/A          \n" +
            "------------------------------------------------------------------------------------------------"
    )));
  }

  @Test
  public void shouldPrintInfoForEmptyMigrationsDir() throws Exception {
    // Given:
    givenMigrations(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verify(logAppender, atLeastOnce()).doAppend(logCaptor.capture());
    final List<String> logMessages = logCaptor.getAllValues().stream()
        .map(LoggingEvent::getRenderedMessage)
        .collect(Collectors.toList());
    assertThat(logMessages, hasItem(containsString("Current migration version: <none>")));
    assertThat(logMessages, hasItem(containsString("No migrations files found")));
  }

  @Test
  public void shouldFailIfMetadataNotInitialized() throws Exception {
    // Given:
    givenMigrations(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());

    when(sourceDescriptionCf.get())
        .thenThrow(new ExecutionException("Source not found", new RuntimeException()));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(1));
  }

  @Test
  public void shouldIssueMultiKeyPullQueryIfSupported() throws Exception {
    // Given:
    givenMigrations(
        ImmutableList.of("1", "3"),
        ImmutableList.of(MIGRATED, MIGRATED),
        ImmutableList.of("N/A", "N/A"),
        ImmutableList.of());

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verify(ksqlClient).executeQuery(
        "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason "
            + "FROM " + MIGRATIONS_TABLE + " WHERE version_key IN ('1', '3');");
  }

  @Test
  public void shouldFallBackToSingleKeyPullQueriesIfNeeded() throws Exception {
    // Given:
    givenMigrations(
        ImmutableList.of("1", "3"),
        ImmutableList.of(MIGRATED, MIGRATED),
        ImmutableList.of("N/A", "N/A"),
        ImmutableList.of(),
        false);

    when(serverInfo.getServerVersion()).thenReturn("v0.13.0");

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verify(ksqlClient).executeQuery(
        "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason "
            + "FROM " + MIGRATIONS_TABLE + " WHERE version_key = '1';");
    verify(ksqlClient).executeQuery(
        "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason "
            + "FROM " + MIGRATIONS_TABLE + " WHERE version_key = '3';");
  }

  /**
   * @param versions versions, in the order they were applied
   * @param states corresponding migration states (ordered according to {@code versions})
   * @param errorReasons corresponding error reasons (ordered according to {@code versions})
   * @param unappliedVersions (additional) existing versions, that have not been applied
   */
  private void givenMigrations(
      final List<String> versions,
      final List<MigrationState> states,
      final List<String> errorReasons,
      final List<String> unappliedVersions
  ) throws Exception {
    givenMigrations(versions, states, errorReasons, unappliedVersions, true);
  }

  /**
   * @param appliedVersions applied versions, in the order they were applied
   * @param states corresponding migration states (ordered according to {@code versions})
   * @param errorReasons corresponding error reasons (ordered according to {@code versions})
   * @param unappliedVersions (additional) existing versions, that have not been applied
   * @param multiKeyPullQuerySupported whether the server version supports multi-key pull queries
   */
  private void givenMigrations(
      final List<String> appliedVersions,
      final List<MigrationState> states,
      final List<String> errorReasons,
      final List<String> unappliedVersions,
      final boolean multiKeyPullQuerySupported
  ) throws Exception {
    givenExistingMigrationFiles(appliedVersions);
    givenExistingMigrationFiles(unappliedVersions);
    givenCurrentMigrationVersion(
        appliedVersions.size() > 0
            ? appliedVersions.get(appliedVersions.size() - 1)
            : MetadataUtil.NONE_VERSION);

    final List<Row> appliedRows = new ArrayList<>();
    for (int i = 0; i < appliedVersions.size(); i++) {
      String version = appliedVersions.get(i);
      String prevVersion = i > 0 ? appliedVersions.get(i-1) : MetadataUtil.NONE_VERSION;

      Row row = mock(Row.class);
      when(row.getString(1)).thenReturn(version);
      when(row.getString(2)).thenReturn("checksum");
      when(row.getString(3)).thenReturn(prevVersion);
      when(row.getString(4)).thenReturn(states.get(i).toString());
      when(row.getString(5)).thenReturn(fileDescriptionForVersion(version));
      when(row.getString(6)).thenReturn("N/A");
      when(row.getString(7)).thenReturn("N/A");
      when(row.getString(8)).thenReturn(errorReasons.get(i));
      appliedRows.add(row);
    }

    if (multiKeyPullQuerySupported) {
      BatchedQueryResult queryResult = mock(BatchedQueryResult.class);
      when(ksqlClient.executeQuery(
          "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason "
              + "FROM " + MIGRATIONS_TABLE + " WHERE version_key IN ('"
              + Stream.concat(appliedVersions.stream(), unappliedVersions.stream()).collect(Collectors.joining("', '"))
              + "');"))
          .thenReturn(queryResult);
      when(queryResult.get()).thenReturn(appliedRows);
    } else {
      for (int i = 0; i < appliedVersions.size(); i++) {
        BatchedQueryResult queryResult = mock(BatchedQueryResult.class);
        when(ksqlClient.executeQuery(
            "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason FROM "
                + MIGRATIONS_TABLE + " WHERE version_key = '" + appliedVersions.get(i) + "';"))
            .thenReturn(queryResult);
        when(queryResult.get()).thenReturn(ImmutableList.of(appliedRows.get(i)));
      }
      for (String version : unappliedVersions) {
        BatchedQueryResult queryResult = mock(BatchedQueryResult.class);
        when(ksqlClient.executeQuery(
            "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason FROM "
                + MIGRATIONS_TABLE + " WHERE version_key = '" + version + "';"))
            .thenReturn(queryResult);
        when(queryResult.get()).thenReturn(ImmutableList.of());
      }
    }
  }

  private void givenExistingMigrationFiles(final List<String> versions) throws Exception {
    for (final String version : versions) {
      final String filename = filePathForVersion(version);
      assertThat(new File(filename).createNewFile(), is(true));
    }
  }

  private void givenCurrentMigrationVersion(final String version) throws Exception {
    Row row = mock(Row.class);
    BatchedQueryResult queryResult = mock(BatchedQueryResult.class);
    when(ksqlClient.executeQuery("SELECT VERSION FROM " + MIGRATIONS_TABLE
        + " WHERE version_key = '" + CURRENT_VERSION_KEY + "';"))
        .thenReturn(queryResult);
    when(queryResult.get()).thenReturn(ImmutableList.of(row));
    when(row.getString("VERSION")).thenReturn(version);
  }

  private String fileDescriptionForVersion(final String version) {
    return "some_name_" + version;
  }

  private String filePathForVersion(final String version) {
    final String prefix = getFilePrefixForVersion(version);
    return Paths.get(migrationsDir, prefix + "__" + fileDescriptionForVersion(version) + ".sql").toString();
  }
}