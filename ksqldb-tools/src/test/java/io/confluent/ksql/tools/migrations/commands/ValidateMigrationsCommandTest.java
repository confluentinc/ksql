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

import static io.confluent.ksql.tools.migrations.util.MetadataUtil.CURRENT_VERSION_KEY;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getFilePrefixForVersion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.github.rvesse.airline.SingleCommand;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValidateMigrationsCommandTest {

  private static final SingleCommand<ValidateMigrationsCommand> PARSER =
      SingleCommand.singleCommand(ValidateMigrationsCommand.class);

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
  private SourceDescription sourceDescription;

  private String migrationsDir;
  private ValidateMigrationsCommand command;

  @Before
  public void setUp() throws Exception {
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME)).thenReturn(MIGRATIONS_STREAM);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME)).thenReturn(MIGRATIONS_TABLE);
    when(ksqlClient.describeSource(MIGRATIONS_STREAM)).thenReturn(sourceDescriptionCf);
    when(ksqlClient.describeSource(MIGRATIONS_TABLE)).thenReturn(sourceDescriptionCf);
    when(sourceDescriptionCf.get()).thenReturn(sourceDescription);

    migrationsDir = folder.getRoot().getPath();
    command = PARSER.parse();
  }

  @Test
  public void shouldValidateSingleMigration() throws Exception {
    // Given:
    final List<String> versions = ImmutableList.of("1");
    final List<String> checksums = givenExistingMigrationFiles(versions);
    givenAppliedMigrations(versions, checksums);

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verifyClientCallsForVersions(versions);
  }

  @Test
  public void shouldValidateMultipleMigrations() throws Exception {
    // Given:
    final List<String> versions = ImmutableList.of("1", "2", "3");
    final List<String> checksums = givenExistingMigrationFiles(versions);
    givenAppliedMigrations(versions, checksums);

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verifyClientCallsForVersions(versions);
  }

  @Test
  public void shouldValidateNoMigrations() throws Exception {
    // Given:
    final List<String> versions = ImmutableList.of();
    final List<String> checksums = givenExistingMigrationFiles(versions);
    givenAppliedMigrations(versions, checksums);

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verifyClientCallsForVersions(versions);
  }

  @Test
  public void shouldValidateWithExtraMigrationFiles() throws Exception {
    // Given:
    final List<String> migratedVersions = ImmutableList.of("1", "2", "4");
    final List<String> migrationFiles = ImmutableList.of("1", "2", "3", "4", "5");
    final List<String> allChecksums = givenExistingMigrationFiles(migrationFiles);
    givenAppliedMigrations(migratedVersions, ImmutableList.of(allChecksums.get(0), allChecksums.get(1), allChecksums.get(3)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    verifyClientCallsForVersions(migratedVersions);
  }

  @Test
  public void shouldFailOnMissingMigrationFile() throws Exception {
    // Given:
    final List<String> migratedVersions = ImmutableList.of("1", "2", "3");
    final List<String> migrationFiles = ImmutableList.of("1", "3");
    final List<String> checksums = givenExistingMigrationFiles(migrationFiles);
    givenAppliedMigrations(migratedVersions, ImmutableList.of(checksums.get(0), "missing", checksums.get(1)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(1));

    // verification stops on failure, so version "1" is never queried
    verifyClientCallsForVersions(ImmutableList.of("2", "3"));
  }

  @Test
  public void shouldFailOnChecksumMismatch() throws Exception {
    // Given:
    final List<String> versions = ImmutableList.of("1", "2", "3");
    final List<String> checksums = givenExistingMigrationFiles(versions);
    givenAppliedMigrations(versions, ImmutableList.of(checksums.get(0), "mismatched_checksum", checksums.get(2)));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(1));

    // verification stops on failure, so version "1" is never queried
    verifyClientCallsForVersions(ImmutableList.of("2", "3"));
  }

  @Test
  public void shouldNotValidateCurrentVersionIfNotMigrated() throws Exception {
    // Given:
    final List<String> versions = ImmutableList.of("1", "2", "3");
    final List<String> checksums = givenExistingMigrationFiles(versions);
    final List<MigrationState> states = ImmutableList.of(MigrationState.MIGRATED, MigrationState.MIGRATED, MigrationState.ERROR);
    givenAppliedMigrations(versions, checksums, states);

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(0));

    //
    verifyClientCallsForVersions(versions, "2");
  }

  @Test
  public void shouldFailIfMetadataNotInitialized() throws Exception {
    // Given:
    final List<String> versions = ImmutableList.of();
    final List<String> checksums = givenExistingMigrationFiles(versions);
    givenAppliedMigrations(versions, checksums);

    when(sourceDescriptionCf.get())
        .thenThrow(new ExecutionException("Source not found", new RuntimeException()));

    // When:
    final int result = command.command(config, cfg -> ksqlClient, migrationsDir);

    // Then:
    assertThat(result, is(1));
  }

  /**
   * @return checksums for the supplied versions
   */
  private List<String> givenExistingMigrationFiles(final List<String> versions) throws Exception {
    final List<String> checksums = new ArrayList<>();

    for (final String version : versions) {
      final String filename = filePathForVersion(version);
      final String fileContents = fileContentsForVersion(version);

      assertThat(new File(filename).createNewFile(), is(true));

      try (PrintWriter out = new PrintWriter(filename, Charset.defaultCharset().name())) {
        out.println(fileContents);
      } catch (FileNotFoundException | UnsupportedEncodingException e) {
        Assert.fail("Failed to write test file: " + filename);
      }

      checksums.add(MigrationsDirectoryUtil.computeHashForFile(filename));
    }

    return checksums;
  }

  /**
   * @param versions versions, in the order they were applied
   * @param checksums corresponding checksums (ordered according to {@code versions})
   */
  private void givenAppliedMigrations(
      final List<String> versions,
      final List<String> checksums
  ) throws Exception {
    givenAppliedMigrations(
        versions,
        checksums,
        Collections.nCopies(versions.size(), MigrationState.MIGRATED)
    );
  }

  /**
   * @param versions versions, in the order they were applied
   * @param checksums corresponding checksums (ordered according to {@code versions})
   * @param states corresponding migration states (ordered according to {@code versions})
   */
  private void givenAppliedMigrations(
      final List<String> versions,
      final List<String> checksums,
      final List<MigrationState> states
  ) throws Exception {
    String version = versions.size() > 0
        ? versions.get(versions.size() - 1)
        : MetadataUtil.NONE_VERSION;

    Row row = mock(Row.class);
    BatchedQueryResult queryResult = mock(BatchedQueryResult.class);
    when(ksqlClient.executeQuery("SELECT VERSION FROM " + MIGRATIONS_TABLE
        + " WHERE version_key = '" + CURRENT_VERSION_KEY + "';"))
        .thenReturn(queryResult);
    when(queryResult.get()).thenReturn(ImmutableList.of(row));
    when(row.getString("VERSION")).thenReturn(version);

    for (int i = versions.size() - 1; i >= 0; i--) {
      version = versions.get(i);
      String prevVersion = i > 0 ? versions.get(i-1) : MetadataUtil.NONE_VERSION;

      row = mock(Row.class);
      queryResult = mock(BatchedQueryResult.class);
      when(ksqlClient.executeQuery(
          "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason FROM "
              + MIGRATIONS_TABLE + " WHERE version_key = '" + version + "';"))
          .thenReturn(queryResult);
      when(queryResult.get()).thenReturn(ImmutableList.of(row));
      when(row.getString(1)).thenReturn(version);
      when(row.getString(2)).thenReturn(checksums.get(i));
      when(row.getString(3)).thenReturn(prevVersion);
      when(row.getString(4)).thenReturn(states.get(i).toString());
      when(row.getString(5)).thenReturn("name");
      when(row.getString(6)).thenReturn("N/A");
      when(row.getString(7)).thenReturn("N/A");
      when(row.getString(8)).thenReturn("no_error");
    }
  }

  /**
   * @param versions versions, in the order they were applied
   */
  private void verifyClientCallsForVersions(final List<String> versions) {
    final String lastVersion = versions.size() > 0 ? versions.get(versions.size() - 1) : "N/A";
    verifyClientCallsForVersions(versions, lastVersion);
  }

  /**
   * @param versions versions, in the order they were applied
   * @param latestMigratedVersion latest migrated version, always either the last version or the
   *                              second-to-last version in {@code versions}. Info for this
   *                              version is fetched twice by the algorithm for `validate`.
   */
  private void verifyClientCallsForVersions(
      final List<String> versions,
      final String latestMigratedVersion
  ) {
    final InOrder inOrder = inOrder(ksqlClient);

    // call to get latest version
    inOrder.verify(ksqlClient).executeQuery("SELECT VERSION FROM " + MIGRATIONS_TABLE
        + " WHERE version_key = '" + CURRENT_VERSION_KEY + "';");

    // calls to get info for migrated versions
    for (int i = versions.size() - 1; i >= 0; i--) {
      final int expectedTimes = versions.get(i).equals(latestMigratedVersion) ? 2 : 1;
      inOrder.verify(ksqlClient, times(expectedTimes)).executeQuery(
          "SELECT version, checksum, previous, state, name, started_on, completed_on, error_reason FROM "
              + MIGRATIONS_TABLE + " WHERE version_key = '" + versions.get(i) + "';");
    }

    // close the client
    inOrder.verify(ksqlClient).close();

    inOrder.verifyNoMoreInteractions();
  }

  private String filePathForVersion(final String version) {
    final String prefix = getFilePrefixForVersion(version);
    return Paths.get(migrationsDir, prefix + "__awesome_migration.sql").toString();
  }

  private static String fileContentsForVersion(final String version) {
    return "sql_" + version;
  }

}