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

import static io.confluent.ksql.tools.migrations.MigrationsUtil.MIGRATIONS_CONFIG_FILE;
import static io.confluent.ksql.tools.migrations.MigrationsUtil.MIGRATIONS_DIR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.github.rvesse.airline.SingleCommand;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NewMigrationCommandTest {

  private static final SingleCommand<NewMigrationCommand> parser =
      SingleCommand.singleCommand(NewMigrationCommand.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String testDir;
  private NewMigrationCommand command;

  @Before
  public void setUp() {
    testDir = Paths.get(folder.getRoot().getPath(), "test_dir").toString();
    command = parser.parse(testDir);
  }

  @Test
  public void shouldCreateRootDirectory() {
    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    final File expectedDir = new File(testDir);
    assertThat(expectedDir.exists(), is(true));
    assertThat(expectedDir.isDirectory(), is(true));
  }

  @Test
  public void shouldCreateMigrationsDirectory() {
    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    final File expectedDir = new File(Paths.get(testDir, MIGRATIONS_DIR).toString());
    assertThat(expectedDir.exists(), is(true));
    assertThat(expectedDir.isDirectory(), is(true));
  }

  @Test
  public void shouldCreateConfigFile() {
    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    final File expectedFile = new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString());
    assertThat(expectedFile.exists(), is(true));
    assertThat(expectedFile.isDirectory(), is(false));
  }

  @Test
  public void shouldHandleArgWithTrailingSlash() {
    // Given:
    testDir = Paths.get(folder.getRoot().getPath(), "test_dir").toString();
    command = parser.parse(testDir + "/");

    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    assertThat(new File(testDir).exists(), is(true));
    assertThat(new File(Paths.get(testDir, MIGRATIONS_DIR).toString()).exists(), is(true));
    assertThat(new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString()).exists(), is(true));
  }

  @Test
  public void shouldHandleArgWithSubDir() {
    // Given:
    testDir = Paths.get(folder.getRoot().getPath(), "test_dir/sub_test_dir").toString();
    command = parser.parse(testDir);

    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    assertThat(new File(testDir).exists(), is(true));
    assertThat(new File(Paths.get(testDir, MIGRATIONS_DIR).toString()).exists(), is(true));
    assertThat(new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString()).exists(), is(true));
  }

  @Test
  public void shouldNotFailIfDirectoriesAlreadyExist() throws Exception {
    // Given:
    Files.createDirectories(Paths.get(testDir));
    Files.createDirectories(Paths.get(testDir, MIGRATIONS_DIR));

    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    assertThat(new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString()).exists(), is(true));
  }

  @Test
  public void shouldNotFailIfConfigFileAlreadyExist() throws Exception {
    // Given:
    Files.createDirectories(Paths.get(testDir));
    new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString()).createNewFile();

    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(0));

    assertThat(new File(Paths.get(testDir, MIGRATIONS_DIR).toString()).exists(), is(true));
  }

  @Test
  public void shouldFailIfRootDirExistsAsFile() throws Exception {
    // Given:
    new File(testDir).createNewFile();

    // When:
    final int status = command.run();

    // Then:
    assertThat(status, is(1));
  }
}