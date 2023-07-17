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

package io.confluent.ksql.tools.migrations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import com.github.rvesse.airline.Cli;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MigrationsParsingTest {

  private static final Cli<Runnable> MIGRATIONS_CLI = new Cli<>(Migrations.class);

  private static final String CONFIG_FILE_PATH = "/path/to/ksql-migrations.properties";
  private static final String UTF_8 = "UTF-8";

  private final ByteArrayOutputStream systemOut = new ByteArrayOutputStream();

  @Before
  public void setUp() throws Exception {
    System.setOut(new PrintStream(systemOut, true, UTF_8));
  }

  @After
  public void tearDown() {
    System.setOut(System.out);
  }

  @Test
  public void shouldPrintHelpIfNoArgs() throws Exception {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{});

    // Then:
    validateGlobalHelpPrinted();
  }

  @Test
  public void shouldPrintHelpOnGlobalHelpOptionLong() throws Exception {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{"--help"});

    // Then:
    validateGlobalHelpPrinted();
  }

  @Test
  public void shouldPrintHelpOnGlobalHelpOptionShort() throws Exception {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{"-h"});

    // Then:
    validateGlobalHelpPrinted();
  }

  @Test
  public void shouldPrintHelpForSpecificCommandLong() throws Exception {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{"apply", "--help"});

    // Then:
    validateCommandHelpPrinted();
  }

  @Test
  public void shouldPrintHelpForSpecificCommandShort() throws Exception {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{"apply", "-h"});

    // Then:
    validateCommandHelpPrinted();
  }

  @Test
  public void shouldAcceptConfigFileBeforeCommand() {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{"--config-file", CONFIG_FILE_PATH, "info"});

    // Then: no exception
  }

  @Test
  public void shouldAcceptConfigFileAfterCommand() {
    // When:
    Migrations.parseCommandFromArgs(MIGRATIONS_CLI, new String[]{"info", "--config-file", CONFIG_FILE_PATH});

    // Then: no exception
  }

  @Test
  public void shouldNotAcceptConfigFileBothBeforeAndAfterCommand() {
    // When:
    final Exception e = assertThrows(MigrationException.class,
        () -> Migrations.parseCommandFromArgs(MIGRATIONS_CLI,
            new String[]{"--config-file", CONFIG_FILE_PATH, "info", "--config-file", CONFIG_FILE_PATH}));

    // Then:
    assertThat(e.getMessage(), containsString("Only one of the following options may be specified but 2 were found"));
  }

  private void validateGlobalHelpPrinted() throws Exception {
    assertThat(systemOut.toString(UTF_8), containsString(
        "ksql-migrations [ {-c | --config-file} <config-file> ] <command> [ <args> ]"));
  }

  private void validateCommandHelpPrinted() throws Exception {
    assertThat(systemOut.toString(UTF_8), containsString(
        "ksql-migrations apply - Migrates the metadata schema to a new schema"));
  }
}