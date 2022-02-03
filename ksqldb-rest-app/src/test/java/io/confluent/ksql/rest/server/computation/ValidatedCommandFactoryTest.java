/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.computation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommandResult;
import io.confluent.ksql.execution.ddl.commands.DropSourceCommand;
import io.confluent.ksql.execution.ddl.commands.Executor;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.util.TerminateCluster;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ValidatedCommandFactoryTest {

  private static final QueryId QUERY_ID = new QueryId("FOO");
  private static final KsqlPlan A_PLAN = KsqlPlan.ddlPlanCurrent(
      "DROP TABLE Bob",
      new DropSourceCommand(SourceName.of("BOB"))
  );

  @Mock
  private KsqlExecutionContext executionContext;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private TerminateQuery terminateQuery;
  @Mock
  private AlterSystemProperty alterSystemProperty;
  @Mock
  private CreateStream plannedQuery;
  @Mock
  private KsqlConfig config;
  @Mock
  private Map<String, Object> overrides;
  @Mock
  private PersistentQueryMetadata query1;
  @Mock
  private PersistentQueryMetadata query2;
  @Mock
  private KsqlExecutionContext.ExecuteResult result;

  private ConfiguredStatement<? extends Statement> configuredStatement;
  private ValidatedCommandFactory commandFactory;

  @Before
  public void setup() {
    commandFactory = new ValidatedCommandFactory();
    when(executionContext.getKsqlConfig()).thenReturn(config);
    when(executionContext.execute(any(), any(ConfiguredKsqlPlan.class))).thenReturn(result);
    when(result.getQuery()).thenReturn(Optional.empty());
  }

  @Test
  public void shouldValidateTerminateCluster() {
    // Given:
    configuredStatement = configuredStatement(
        TerminateCluster.TERMINATE_CLUSTER_STATEMENT_TEXT,
        terminateQuery
    );

    // When:
    final Command command = commandFactory.create(configuredStatement, executionContext);

    // Then:
    assertThat(command, is(Command.of(configuredStatement)));
  }

  @Test
  public void shouldRaiseExceptionIfKeyDoesNotExistEditablePropertiesList() {
    configuredStatement = configuredStatement("ALTER SYSTEM 'ksql.streams.upgrade.from'='TEST';" , alterSystemProperty);
    when(alterSystemProperty.getPropertyName()).thenReturn("ksql.streams.upgrade.from");
    when(alterSystemProperty.getPropertyValue()).thenReturn("TEST");
    when(config.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    commandFactory.create(configuredStatement, executionContext);
  }

  @Test
  public void shouldRaiseExceptionWhenFeatureFlagIsTurnedOff() {
    configuredStatement = configuredStatement("ALTER SYSTEM 'ksql.streams.upgrade.from'='TEST';" , alterSystemProperty);
    when(alterSystemProperty.getPropertyName()).thenReturn("ksql.streams.upgrade.from");
    when(alterSystemProperty.getPropertyValue()).thenReturn("TEST");
    when(config.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(false);

    assertThrows(KsqlServerException.class,
        () -> commandFactory.create(configuredStatement, executionContext));
  }

  @Test
  public void shouldNotRaiseExceptionWhenPrefixIsAdded() {
    configuredStatement = configuredStatement("ALTER SYSTEM 'TEST'='TEST';" , alterSystemProperty);
    when(alterSystemProperty.getPropertyName()).thenReturn("TEST");
    when(alterSystemProperty.getPropertyValue()).thenReturn("TEST");
    when(config.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    assertThrows(ConfigException.class,
        () -> commandFactory.create(configuredStatement, executionContext));
  }

  @Test
  public void shouldRaiseExceptionWhenQueryIsRunningAndProcessingGuranteeIsAttemptedToChange() {
    configuredStatement = configuredStatement("ALTER SYSTEM 'processing.guarantee'='exactly_once';" , alterSystemProperty);
    when(alterSystemProperty.getPropertyName()).thenReturn("processing.guarantee");
    when(alterSystemProperty.getPropertyValue()).thenReturn("exactly_once");
    when(config.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    final List<PersistentQueryMetadata> persistentList = new ArrayList<>();
    persistentList.add(query1);
    when(executionContext.getPersistentQueries()).thenReturn(persistentList);

    assertThrows(ConfigException.class,
        () -> commandFactory.create(configuredStatement, executionContext));
  }

  @Test
  public void shouldFailTerminateSourceTableQuery() {
    // Given:
    configuredStatement = configuredStatement("TERMINATE X", terminateQuery);
    when(terminateQuery.getQueryId()).thenReturn(Optional.of(QUERY_ID));
    when(executionContext.getPersistentQuery(QUERY_ID)).thenReturn(Optional.of(query1));
    when(query1.getPersistentQueryType())
        .thenReturn(KsqlConstants.PersistentQueryType.CREATE_SOURCE);

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> commandFactory.create(configuredStatement, executionContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot terminate query 'FOO' because it is linked to a source table"));
    verify(query1, times(0)).close();
  }

  @Test
  public void shouldFailValidationForTerminateUnknownQuery() {
    // Given:
    configuredStatement = configuredStatement("TERMINATE X", terminateQuery);
    when(terminateQuery.getQueryId()).thenReturn(Optional.of(QUERY_ID));
    when(executionContext.getPersistentQuery(QUERY_ID)).thenReturn(Optional.empty());

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> commandFactory.create(configuredStatement, executionContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown queryId"));

  }

  @Test
  public void shouldCreateCommandForTerminateQuery() {
    // Given:
    givenTerminate();

    // When:
    final Command command = commandFactory.create(configuredStatement, executionContext);

    // Then:
    assertThat(command, is(Command.of(configuredStatement)));
  }

  @Test
  public void shouldValidateTerminateQuery() {
    // Given:
    givenTerminate();

    // When:
    commandFactory.create(configuredStatement, executionContext);

    // Then:
    verify(executionContext).getPersistentQuery(QUERY_ID);
    verify(query1).close();
  }

  @Test
  public void shouldValidateTerminateAllQuery() {
    // Given:
    givenTerminateAll();

    // When:
    commandFactory.create(configuredStatement, executionContext);

    // Then:
    verify(query1).close();
    verify(query2).close();
  }

  @Test
  public void shouldCreateCommandForTerminateAllQuery() {
    // Given:
    givenTerminateAll();

    // When:
    final Command command = commandFactory.create(configuredStatement, executionContext);

    // Then:
    assertThat(command, is(Command.of(configuredStatement)));
  }

  @Test
  public void shouldValidatePlannedQuery() {
    // Given:
    givenPlannedQuery();

    // When:
    commandFactory.create(configuredStatement, executionContext);

    // Then:
    verify(executionContext).plan(serviceContext, configuredStatement);
    verify(executionContext).execute(
        serviceContext,
        ConfiguredKsqlPlan.of(A_PLAN, SessionConfig.of(config, overrides))
    );
  }

  @Test
  public void shouldCreateCommandForPlannedQuery() {
    // Given:
    givenPlannedQuery();

    // When:
    final Command command = commandFactory.create(configuredStatement, executionContext);

    // Then:
    assertThat(command, is(Command.of(ConfiguredKsqlPlan.of(A_PLAN, SessionConfig.of(config, overrides)))));
  }

  @Test
  public void shouldCreateCommandForPlannedQueryInSharedRuntime() {
    // Given:
    givenPlannedQuery();
    BinPackedPersistentQueryMetadataImpl queryMetadata = mock(BinPackedPersistentQueryMetadataImpl.class);
    when(executionContext.execute(any(), any(ConfiguredKsqlPlan.class))).thenReturn(result);
    when(result.getQuery()).thenReturn(Optional.ofNullable(queryMetadata));

    // When:
    final Command command = commandFactory.create(configuredStatement, executionContext);

    // Then:
    assertThat(command, is(Command.of(ConfiguredKsqlPlan.of(A_PLAN, SessionConfig.of(config, overrides)))));

  }

  @Test
  public void shouldCreateCommandForPlannedQueryInDedicatedRuntime() {
    // Given:
    givenPlannedQuery();
    PersistentQueryMetadataImpl queryMetadata = mock(PersistentQueryMetadataImpl.class);
    when(executionContext.execute(any(), any(ConfiguredKsqlPlan.class))).thenReturn(result);
    when(result.getQuery()).thenReturn(Optional.ofNullable(queryMetadata));
    when(config.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(true);

    // When:
    final Command command = commandFactory.create(configuredStatement, executionContext);

    // Then:
    assertThat(command,
        is(Command.of(
            ConfiguredKsqlPlan.of(
                A_PLAN,
                SessionConfig.of(config,
                    ImmutableMap.of(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, false))))));
  }

  @Test
  public void shouldThrowIfCommandCanNotBeDeserialized() {
    // Given:
    givenNonDeserializableCommand();

    // When:
    final Exception e = assertThrows(
        KsqlServerException.class,
        () -> commandFactory.create(configuredStatement, executionContext)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Did not write the command to the command topic "
        + "as it could not be deserialized."));
  }

  private void givenTerminate() {
    configuredStatement = configuredStatement("TERMINATE FOO", terminateQuery);
    when(terminateQuery.getQueryId()).thenReturn(Optional.of(QUERY_ID));
    when(executionContext.getPersistentQuery(any())).thenReturn(Optional.of(query1));
  }

  private void givenTerminateAll() {
    configuredStatement = configuredStatement("TERMINATE ALL", terminateQuery);
    when(terminateQuery.getQueryId()).thenReturn(Optional.empty());
    when(executionContext.getPersistentQueries()).thenReturn(ImmutableList.of(query1, query2));
  }

  private void givenPlannedQuery() {
    configuredStatement = configuredStatement("CREATE STREAM", plannedQuery);
    when(executionContext.plan(any(), any())).thenReturn(A_PLAN);
    when(executionContext.getServiceContext()).thenReturn(serviceContext);
  }

  private void givenNonDeserializableCommand() {
    configuredStatement = configuredStatement("CREATE STREAM", plannedQuery);
    final KsqlPlan planThatFailsToDeserialize = KsqlPlan
        .ddlPlanCurrent("some sql", new UnDeserializableCommand());
    when(executionContext.plan(any(), any())).thenReturn(planThatFailsToDeserialize);
    when(executionContext.getServiceContext()).thenReturn(serviceContext);
  }

  private <T extends Statement> ConfiguredStatement<T> configuredStatement(
      final String text,
      final T statement
  ) {
    return ConfiguredStatement.of(PreparedStatement.of(text, statement),
        SessionConfig.of(config, overrides));
  }

  // Not a known subtype so will fail to deserialize:
  private static class UnDeserializableCommand implements DdlCommand {

    @Override
    public DdlCommandResult execute(final Executor executor) {
      return null;
    }
  }
}
