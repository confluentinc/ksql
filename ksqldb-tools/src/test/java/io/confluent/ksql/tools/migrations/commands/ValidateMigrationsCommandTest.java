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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.rvesse.airline.SingleCommand;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import io.confluent.ksql.tools.migrations.util.MetadataUtil;
import io.confluent.ksql.tools.migrations.util.MetadataUtil.MigrationState;
import io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
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
public class ValidateMigrationsCommandTest {

  private static final SingleCommand<ValidateMigrationsCommand> PARSER =
      SingleCommand.singleCommand(ValidateMigrationsCommand.class);

  private static final String MIGRATIONS_TABLE = "migrations_table";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Mock
  private MigrationConfig config;
  @Mock
  private Client ksqlClient;

  private String migrationsDir;
  private ValidateMigrationsCommand command;

  @Before
  public void setUp() {
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME)).thenReturn(MIGRATIONS_TABLE);

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
   * @parma states corresponding migration states (ordered according to {@code versions})
   */
  private void givenAppliedMigrations(
      final List<String> versions,
      final List<String> checksums,
      final List<MigrationState> states
  ) throws Exception {
    String version = versions.get(versions.size() - 1);

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
      when(ksqlClient.executeQuery("SELECT checksum, previous, state FROM " + MIGRATIONS_TABLE
          + " WHERE version_key = '" + version + "';"))
          .thenReturn(queryResult);
      when(queryResult.get()).thenReturn(ImmutableList.of(row));
      when(row.getString(0)).thenReturn(checksums.get(i));
      when(row.getString(1)).thenReturn(prevVersion);
      when(row.getString(2)).thenReturn(states.get(i).toString());
    }
  }

  /**
   * @param versions versions, in the order they were applied
   */
  private void verifyClientCallsForVersions(final List<String> versions) {
    final InOrder inOrder = inOrder(ksqlClient);

    // call to get latest version
    inOrder.verify(ksqlClient).executeQuery("SELECT VERSION FROM " + MIGRATIONS_TABLE
        + " WHERE version_key = '" + CURRENT_VERSION_KEY + "';");

    // call to get info for latest version
    inOrder.verify(ksqlClient, times(2)).executeQuery("SELECT checksum, previous, state FROM " + MIGRATIONS_TABLE
        + " WHERE version_key = '" + versions.get(versions.size() - 1) + "';");

    // calls to get all migrated versions
    for (int i = versions.size() - 2; i >= 0; i--) {
      inOrder.verify(ksqlClient).executeQuery("SELECT checksum, previous, state FROM " + MIGRATIONS_TABLE
          + " WHERE version_key = '" + versions.get(i) + "';");
    }

    // close the client
    inOrder.verify(ksqlClient).close();

    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldValidateMultipleMigrations() throws Exception {

  }

  @Test
  public void shouldValidateNoMigrations() throws Exception {

  }

  @Test
  public void shouldValidateWithExtraMigrations() throws Exception {

  }

  @Test
  public void shouldFailOnMissingMigrationFile() throws Exception {

  }

  @Test
  public void shouldFailOnChecksumMismatch() throws Exception {

  }

  @Test
  public void shouldNotValidateCurrentIfNotMigrated() throws Exception {

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
      } catch (Exception e) {
        Assert.fail("Failed to write test file: " + filename);
      }

      checksums.add(MigrationsDirectoryUtil.computeHashForFile(filename));
    }

    return checksums;
  }

  private String filePathForVersion(final String version) {
    final String prefix = "V" + StringUtils.leftPad(version, 6, "0");
    return Paths.get(migrationsDir, prefix + "_awesome_migration").toString();
  }

  private static String fileContentsForVersion(final String version) {
    return "sql_" + version;
  }

}