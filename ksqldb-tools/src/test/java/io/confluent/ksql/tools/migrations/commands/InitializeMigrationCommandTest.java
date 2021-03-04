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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.rvesse.airline.SingleCommand;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InitializeMigrationCommandTest {

  private static final SingleCommand<InitializeMigrationCommand> PARSER =
      SingleCommand.singleCommand(InitializeMigrationCommand.class);

  private static final String MIGRATIONS_STREAM = "migrations_stream";
  private static final String MIGRATIONS_TABLE = "migrations_table";
  private static final String MIGRATIONS_STREAM_TOPIC = "migrations_stream_topic";
  private static final String MIGRATIONS_TABLE_TOPIC = "migrations_table_topic";
  private static final int TOPIC_REPLICAS = 2;

  private static final String EXPECTED_CS_STATEMENT =
      "CREATE STREAM " + MIGRATIONS_STREAM + " (\n"
          + "  version_key  STRING KEY,\n"
          + "  version      STRING,\n"
          + "  name         STRING,\n"
          + "  state        STRING,  \n"
          + "  checksum     STRING,\n"
          + "  started_on   STRING,\n"
          + "  completed_on STRING,\n"
          + "  previous     STRING,\n"
          + "  error_reason STRING\n"
          + ") WITH (  \n"
          + "  KAFKA_TOPIC='" + MIGRATIONS_STREAM_TOPIC + "',\n"
          + "  VALUE_FORMAT='JSON',\n"
          + "  PARTITIONS=1,\n"
          + "  REPLICAS= " + TOPIC_REPLICAS + " \n"
          + ");\n";
  private static final String EXPECTED_CTAS_STATEMENT =
      "CREATE TABLE " + MIGRATIONS_TABLE + "\n"
          + "  WITH (\n"
          + "    KAFKA_TOPIC='" + MIGRATIONS_TABLE_TOPIC + "'\n"
          + "  )\n"
          + "  AS SELECT \n"
          + "    version_key, \n"
          + "    latest_by_offset(version) as version, \n"
          + "    latest_by_offset(name) AS name, \n"
          + "    latest_by_offset(state) AS state,     \n"
          + "    latest_by_offset(checksum) AS checksum, \n"
          + "    latest_by_offset(started_on) AS started_on, \n"
          + "    latest_by_offset(completed_on) AS completed_on, \n"
          + "    latest_by_offset(previous) AS previous, \n"
          + "    latest_by_offset(error_reason) AS error_reason \n"
          + "  FROM " + MIGRATIONS_STREAM + " \n"
          + "  GROUP BY version_key;\n";

  @Mock
  private MigrationConfig config;
  @Mock
  private Client client;
  @Mock
  private CompletableFuture<ServerInfo> serverInfoCf;
  @Mock
  private CompletableFuture<ExecuteStatementResult> executeStatementCf;
  @Mock
  private ServerInfo serverInfo;
  @Mock
  private ExecuteStatementResult executeStatementResult;

  private InitializeMigrationCommand command;

  @Before
  public void setUp() throws Exception {
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME)).thenReturn(MIGRATIONS_STREAM);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME)).thenReturn(MIGRATIONS_TABLE);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_TOPIC_NAME)).thenReturn(MIGRATIONS_STREAM_TOPIC);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_TOPIC_NAME)).thenReturn(MIGRATIONS_TABLE_TOPIC);
    when(config.getInt(MigrationConfig.KSQL_MIGRATIONS_TOPIC_REPLICAS)).thenReturn(TOPIC_REPLICAS);
    when(client.serverInfo()).thenReturn(serverInfoCf);
    when(client.executeStatement(anyString())).thenReturn(executeStatementCf);
    when(serverInfoCf.get()).thenReturn(serverInfo);
    when(serverInfo.getServerVersion()).thenReturn("v0.14.0");
    when(executeStatementCf.get()).thenReturn(executeStatementResult);

    command = PARSER.parse();
  }

  @Test
  public void shouldCreateMigrationsStream() {
    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement(EXPECTED_CS_STATEMENT);
  }

  @Test
  public void shouldCreateMigrationsTable() {
    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement(EXPECTED_CTAS_STATEMENT);
  }

  @Test
  public void shouldNotCreateTableIfFailedToCreateStream() throws Exception {
    // Given:
    when(executeStatementCf.get()).thenThrow(new ExecutionException(new RuntimeException("failed")));

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(1));

    verify(client, never()).executeStatement(EXPECTED_CTAS_STATEMENT);
  }

  @Test
  public void shouldNotInitializeIfServerVersionIncompatible() {
    // Given:
    when(serverInfo.getServerVersion()).thenReturn("v0.9.0");

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(1));

    verify(client, never()).executeStatement(EXPECTED_CS_STATEMENT);
    verify(client, never()).executeStatement(EXPECTED_CTAS_STATEMENT);
  }

  @Test
  public void shouldInitializeEvenIfCantParseServerVersion() {
    // Given:
    when(serverInfo.getServerVersion()).thenReturn("not_a_valid_version");

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement(EXPECTED_CS_STATEMENT);
    verify(client).executeStatement(EXPECTED_CTAS_STATEMENT);
  }
}