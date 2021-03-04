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
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.tools.migrations.MigrationConfig;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CleanMigrationsCommandTest {

  private static final SingleCommand<CleanMigrationsCommand> PARSER =
      SingleCommand.singleCommand(CleanMigrationsCommand.class);

  private static final String MIGRATIONS_STREAM = "migrations_stream";
  private static final String MIGRATIONS_TABLE = "migrations_table";
  private static final String CTAS_QUERY_ID = "ctas_migration_table_0";

  @Mock
  private MigrationConfig config;
  @Mock
  private Client client;
  @Mock
  private CompletableFuture<ServerInfo> serverInfoCf;
  @Mock
  private CompletableFuture<ExecuteStatementResult> executeStatementCf;
  @Mock
  private CompletableFuture<List<StreamInfo>> listStreamsCf;
  @Mock
  private CompletableFuture<List<TableInfo>> listTablesCf;
  @Mock
  private CompletableFuture<SourceDescription> describeSourceCf;
  @Mock
  private ServerInfo serverInfo;
  @Mock
  private ExecuteStatementResult executeStatementResult;
  @Mock
  private StreamInfo streamInfo;
  @Mock
  private TableInfo tableInfo;
  @Mock
  private SourceDescription sourceDescription;
  @Mock
  private QueryInfo ctasQueryInfo;
  @Mock
  private QueryInfo otherQueryInfo;

  private CleanMigrationsCommand command;

  @Before
  public void setUp() throws Exception {
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_STREAM_NAME)).thenReturn(MIGRATIONS_STREAM);
    when(config.getString(MigrationConfig.KSQL_MIGRATIONS_TABLE_NAME)).thenReturn(MIGRATIONS_TABLE);
    when(client.serverInfo()).thenReturn(serverInfoCf);
    when(client.executeStatement(anyString())).thenReturn(executeStatementCf);
    when(client.listStreams()).thenReturn(listStreamsCf);
    when(client.listTables()).thenReturn(listTablesCf);
    when(client.describeSource(MIGRATIONS_TABLE)).thenReturn(describeSourceCf);

    when(serverInfoCf.get()).thenReturn(serverInfo);
    when(serverInfo.getServerVersion()).thenReturn("v0.14.0");
    when(executeStatementCf.get()).thenReturn(executeStatementResult);
    when(listStreamsCf.get()).thenReturn(ImmutableList.of(streamInfo));
    when(listTablesCf.get()).thenReturn(ImmutableList.of(tableInfo));
    when(describeSourceCf.get()).thenReturn(sourceDescription);

    when(streamInfo.getName()).thenReturn(MIGRATIONS_STREAM);
    when(tableInfo.getName()).thenReturn(MIGRATIONS_TABLE);
    when(sourceDescription.writeQueries()).thenReturn(ImmutableList.of(ctasQueryInfo));
    when(ctasQueryInfo.getId()).thenReturn(CTAS_QUERY_ID);
    when(otherQueryInfo.getId()).thenReturn("other_query_id");

    command = PARSER.parse();
  }

  @Test
  public void shouldCleanMigrationsStreamAndTable() {
    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement("TERMINATE " + CTAS_QUERY_ID + ";");
    verify(client).executeStatement("DROP TABLE " + MIGRATIONS_TABLE + " DELETE TOPIC;");
    verify(client).executeStatement("DROP STREAM " + MIGRATIONS_STREAM + " DELETE TOPIC;");
  }

  @Test
  public void shouldCleanMigrationsStreamEvenIfTableDoesntExist() throws Exception {
    // Given:
    givenMigrationsTableDoesNotExist();

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement("DROP STREAM " + MIGRATIONS_STREAM + " DELETE TOPIC;");
    verify(client, never()).executeStatement("DROP TABLE " + MIGRATIONS_TABLE + " DELETE TOPIC;");
    verify(client, never()).executeStatement("TERMINATE " + CTAS_QUERY_ID + ";");
  }

  @Test
  public void shouldCleanMigrationsTableEvenIfStreamDoesntExist() throws Exception {
    // Given:
    givenMigrationsStreamDoesNotExist();

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement("DROP TABLE " + MIGRATIONS_TABLE + " DELETE TOPIC;");
    verify(client, never()).executeStatement("DROP STREAM " + MIGRATIONS_STREAM + " DELETE TOPIC;");

    // a single query writing to the table will still be dropped, even if the stream
    // doesn't exist. we could change this in the future but it's an unlikely (and
    // unexpected) edge case that doesn't seem too important.
    verify(client).executeStatement("TERMINATE " + CTAS_QUERY_ID + ";");
  }

  @Test
  public void shouldCleanMigrationsTableEvenIfQueryDoesntExist() {
    // Given:
    when(sourceDescription.writeQueries()).thenReturn(ImmutableList.of());

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement("DROP TABLE " + MIGRATIONS_TABLE + " DELETE TOPIC;");
    verify(client).executeStatement("DROP STREAM " + MIGRATIONS_STREAM + " DELETE TOPIC;");
  }

  @Test
  public void shouldFailIfMultipleQueriesWritingToTable() {
    // Given:
    when(sourceDescription.writeQueries()).thenReturn(ImmutableList.of(ctasQueryInfo, otherQueryInfo));

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(1));

    verify(client, never()).executeStatement("TERMINATE " + CTAS_QUERY_ID + ";");
    verify(client, never()).executeStatement("DROP TABLE " + MIGRATIONS_TABLE + " DELETE TOPIC;");
    verify(client, never()).executeStatement("DROP STREAM " + MIGRATIONS_STREAM + " DELETE TOPIC;");
  }

  @Test
  public void shouldSucceedIfNeverInitialized() throws Exception {
    // Given:
    givenMigrationsStreamDoesNotExist();
    givenMigrationsTableDoesNotExist();

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));
  }

  @Test
  public void shouldNotCleanIfServerVersionIncompatible() {
    // Given:
    when(serverInfo.getServerVersion()).thenReturn("v0.9.0");

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(1));

    verify(client, never()).executeStatement(anyString());
  }

  @Test
  public void shouldCleanEvenIfCantParseServerVersion() {
    // Given:
    when(serverInfo.getServerVersion()).thenReturn("not_a_valid_version");

    // When:
    final int status = command.command(config, cfg -> client);

    // Then:
    assertThat(status, is(0));

    verify(client).executeStatement("TERMINATE " + CTAS_QUERY_ID + ";");
    verify(client).executeStatement("DROP TABLE " + MIGRATIONS_TABLE + " DELETE TOPIC;");
    verify(client).executeStatement("DROP STREAM " + MIGRATIONS_STREAM + " DELETE TOPIC;");
  }

  private void givenMigrationsStreamDoesNotExist() throws Exception {
    when(listStreamsCf.get()).thenReturn(ImmutableList.of());
  }

  private void givenMigrationsTableDoesNotExist() throws Exception {
    when(listTablesCf.get()).thenReturn(ImmutableList.of());
  }
}