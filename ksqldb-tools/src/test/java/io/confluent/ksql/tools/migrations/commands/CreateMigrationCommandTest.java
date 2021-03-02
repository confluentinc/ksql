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

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.getFilePrefixForVersion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.parser.errors.ParseArgumentsMissingException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CreateMigrationCommandTest {

  private static final SingleCommand<CreateMigrationCommand> PARSER =
      SingleCommand.singleCommand(CreateMigrationCommand.class);

  private static final String DESCRIPTION = "migration file description";
  private static final String EXPECTED_FILE_SUFFIX = "migration_file_description.sql";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String migrationsDir;
  private CreateMigrationCommand command;

  @Before
  public void setUp() {
    migrationsDir = folder.getRoot().getPath();
  }

  @Test
  public void shouldCreateWithNoExplicitVersionAndNonEmptyMigrationsDir() throws Exception {
    // Given:
    givenVersionsExist("1", "2", "4");

    command = PARSER.parse(DESCRIPTION);

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(0));

    final File expectedFile = new File(Paths.get(migrationsDir, "V000005__" + EXPECTED_FILE_SUFFIX).toString());
    assertThat(expectedFile.exists(), is(true));
    assertThat(expectedFile.isDirectory(), is(false));
  }

  @Test
  public void shouldCreateWithNoExplicitVersionAndEmptyMigrationsDir() {
    // Given:
    command = PARSER.parse(DESCRIPTION);

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(0));

    final File expectedFile = new File(Paths.get(migrationsDir, "V000001__" + EXPECTED_FILE_SUFFIX).toString());
    assertThat(expectedFile.exists(), is(true));
    assertThat(expectedFile.isDirectory(), is(false));
  }

  @Test
  public void shouldCreateWithExplicitVersion() {
    // Given:
    command = PARSER.parse(DESCRIPTION, "-v", "12");

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(0));

    final File expectedFile = new File(Paths.get(migrationsDir, "V000012__" + EXPECTED_FILE_SUFFIX).toString());
    assertThat(expectedFile.exists(), is(true));
    assertThat(expectedFile.isDirectory(), is(false));
  }

  @Test
  public void shouldFailIfVersionAlreadyExists() throws Exception {
    // Given:
    givenVersionsExist("12");

    command = PARSER.parse(DESCRIPTION, "-v", "12");

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(1));
  }

  @Test
  public void shouldFailOnNegativeVersion() {
    // Given:
    command = PARSER.parse(DESCRIPTION, "-v", "-1");

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(1));
  }

  @Test
  public void shouldFailIfVersionTooLarge() {
    // Given:
    command = PARSER.parse(DESCRIPTION, "-v", "10000000");

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(1));
  }

  @Test
  public void shouldFailOnMissingDescription() {
    // When:
    final Exception e = assertThrows(ParseArgumentsMissingException.class, () -> PARSER.parse());

    // Then:
    assertThat(e.getMessage(), is("Required arguments are missing: 'description'"));
  }

  @Test
  public void shouldFailOnEmptyDescription() {
    // Given:
    command = PARSER.parse("");

    // When:
    final int result = command.command(migrationsDir);

    // Then:
    assertThat(result, is(1));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  private void givenVersionsExist(final String... versions) throws Exception {
    for (final String version : versions) {
      final String prefix = getFilePrefixForVersion(version);
      new File(Paths.get(migrationsDir, prefix + "__some_desc_" + version + ".sql").toString()).createNewFile();
    }
  }
}