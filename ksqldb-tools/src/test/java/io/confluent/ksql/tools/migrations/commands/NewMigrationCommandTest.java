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

import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.MIGRATIONS_CONFIG_FILE;
import static io.confluent.ksql.tools.migrations.util.MigrationsDirectoryUtil.MIGRATIONS_DIR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.github.rvesse.airline.SingleCommand;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NewMigrationCommandTest {

  private static final SingleCommand<NewMigrationCommand> PARSER =
      SingleCommand.singleCommand(NewMigrationCommand.class);

  private static final String KSQL_SERVER_URL = "http://localhost:8088";

  private static final String DEFAULT_CONFIGS = "ksql.server.url=http://localhost:8088\n"
      + "# The key store path\n"
      + "# ssl.keystore.location=null\n"
      + "# The name of the migration table. It defaults to MIGRATION_SCHEMA_VERSIONS\n"
      + "# ksql.migrations.table.name=MIGRATION_SCHEMA_VERSIONS\n"
      + "# The username for the KSQL server\n"
      + "# ksql.auth.basic.username=null\n"
      + "# The password for the KSQL server\n"
      + "# ksql.auth.basic.password=null\n"
      + "# The name of the migration stream topic. It defaults to '<ksql_service_id>ksql_<migrations_stream_name>'\n"
      + "# ksql.migrations.stream.topic.name=ksql-service-idksql_MIGRATION_EVENTS\n"
      + "# The name of the migration table topic. It defaults to '<ksql_service_id>ksql_<migrations_table_name>'\n"
      + "# ksql.migrations.table.topic.name=ksql-service-idksql_MIGRATION_SCHEMA_VERSIONS\n"
      + "# The trust store path\n"
      + "# ssl.truststore.location=null\n"
      + "# The key store password\n"
      + "# ssl.keystore.password=null\n"
      + "# The number of replicas for the migration stream topic. It defaults to 1\n"
      + "# ksql.migrations.topic.replicas=1\n"
      + "# The key password\n"
      + "# ssl.key.password=null\n"
      + "# Whether hostname verification is enabled. It defaults to true.\n"
      + "# ssl.verify.host=true\n"
      + "# The trust store password\n"
      + "# ssl.truststore.password=null\n"
      + "# The key alias\n"
      + "# ssl.key.alias=null\n"
      + "# The name of the migration stream. It defaults to MIGRATION_EVENTS\n"
      + "# ksql.migrations.stream.name=MIGRATION_EVENTS\n"
      + "# Whether ALPN should be used. It defaults to false.\n"
      + "# ssl.alpn=false\n";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private String testDir;
  private NewMigrationCommand command;

  @Before
  public void setUp() {
    testDir = Paths.get(folder.getRoot().getPath(), "test_dir").toString();
    command = PARSER.parse(testDir, KSQL_SERVER_URL);
  }

  @Test
  public void shouldCreateRootDirectory() {
    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(0));

    final File expectedDir = new File(testDir);
    assertThat(expectedDir.exists(), is(true));
    assertThat(expectedDir.isDirectory(), is(true));
  }

  @Test
  public void shouldCreateMigrationsDirectory() {
    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(0));

    final File expectedDir = new File(Paths.get(testDir, MIGRATIONS_DIR).toString());
    assertThat(expectedDir.exists(), is(true));
    assertThat(expectedDir.isDirectory(), is(true));
  }

  @Test
  public void shouldCreateAndWriteToConfigFile() throws Exception {
    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(0));

    final File expectedFile = new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString());
    assertThat(expectedFile.exists(), is(true));
    assertThat(expectedFile.isDirectory(), is(false));

    final String configFile = new String(Files.readAllBytes(expectedFile.toPath()), StandardCharsets.UTF_8);

    assertThat(configFile, is(DEFAULT_CONFIGS));
  }

  @Test
  public void shouldHandleArgWithTrailingSlash() {
    // Given:
    testDir = Paths.get(folder.getRoot().getPath(), "test_dir").toString();
    command = PARSER.parse(testDir + "/", KSQL_SERVER_URL);

    // When:
    final int status = command.runCommand();

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
    command = PARSER.parse(testDir, KSQL_SERVER_URL);

    // When:
    final int status = command.runCommand();

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
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(0));

    assertThat(new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString()).exists(), is(true));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  @Test
  public void shouldFailIfConfigFileAlreadyExists() throws Exception {
    // Given:
    Files.createDirectories(Paths.get(testDir));
    new File(Paths.get(testDir, MIGRATIONS_CONFIG_FILE).toString()).createNewFile();

    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(1));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  @Test
  public void shouldFailIfRootDirExistsAsFile() throws Exception {
    // Given:
    new File(testDir).createNewFile();

    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(1));
  }

  @Test
  public void shouldFailIfTooManyArgs() {
    // Given:
    command = PARSER.parse(testDir, KSQL_SERVER_URL, "other_arg");

    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(1));
  }

  @Test
  public void shouldFailIfTooFewArgs() {
    // Given:
    command = PARSER.parse(testDir);

    // When:
    final int status = command.runCommand();

    // Then:
    assertThat(status, is(1));
  }
}