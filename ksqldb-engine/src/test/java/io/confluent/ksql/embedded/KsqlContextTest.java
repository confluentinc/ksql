/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.embedded;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class KsqlContextTest {

  private static final KsqlConfig SOME_CONFIG = new KsqlConfig(Collections.emptyMap());
  private static final ImmutableMap<String, Object> SOME_PROPERTIES = ImmutableMap
      .of("overridden", "props");

  private final static ParsedStatement PARSED_STMT_0 = ParsedStatement
      .of("sql 0", mock(SingleStatementContext.class));

  private final static ParsedStatement PARSED_STMT_1 = ParsedStatement
      .of("sql 1", mock(SingleStatementContext.class));

  private final static PreparedStatement<?> PREPARED_STMT_0 = PreparedStatement
      .of("sql 0", mock(Statement.class));

  private final static PreparedStatement<?> PREPARED_STMT_1 = PreparedStatement
      .of("sql 1", mock(Statement.class));

  private final static ConfiguredStatement<?> CFG_STMT_0 = ConfiguredStatement.of(
      PREPARED_STMT_0, SessionConfig.of(SOME_CONFIG, SOME_PROPERTIES));

  private final static ConfiguredStatement<?> CFG_STMT_1 = ConfiguredStatement.of(
      PREPARED_STMT_1, SessionConfig.of(SOME_CONFIG, SOME_PROPERTIES));

  private final static ConfiguredStatement<?> STMT_0_WITH_SCHEMA = ConfiguredStatement.of(
      PREPARED_STMT_0, SessionConfig.of(SOME_CONFIG, SOME_PROPERTIES));

  private final static ConfiguredStatement<?> STMT_1_WITH_SCHEMA = ConfiguredStatement.of(
      PREPARED_STMT_1, SessionConfig.of(SOME_CONFIG, SOME_PROPERTIES));

  private final static ConfiguredStatement<?> STMT_0_WITH_TOPIC = ConfiguredStatement.of(
      PREPARED_STMT_0, SessionConfig.of(SOME_CONFIG, SOME_PROPERTIES));

  private final static ConfiguredStatement<?> STMT_1_WITH_TOPIC = ConfiguredStatement.of(
      PREPARED_STMT_1, SessionConfig.of(SOME_CONFIG, SOME_PROPERTIES));

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlExecutionContext sandbox;
  @Mock
  private PersistentQueryMetadata persistentQuery;
  @Mock
  private TransientQueryMetadata transientQuery;
  @Mock
  private Injector schemaInjector;
  @Mock
  private Injector topicInjector;

  private KsqlContext ksqlContext;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0));

    when(ksqlEngine.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(ksqlEngine.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(ksqlEngine.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of("success"));

    when(ksqlEngine.createSandbox(any())).thenReturn(sandbox);

    when(ksqlEngine.getServiceContext()).thenReturn(serviceContext);

    when(sandbox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandbox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    when(topicInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    ksqlContext = new KsqlContext(
        serviceContext,
        SOME_CONFIG,
        ksqlEngine,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector));

  }

  @Test
  public void shouldParseStatements() {
    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).parse("Some SQL");
  }

  @Test
  public void shouldOnlyPrepareNextStatementOncePreviousStatementHasBeenExecuted() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(
        ImmutableList.of(PARSED_STMT_0, PARSED_STMT_1));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine);
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_0);
    inOrder.verify(ksqlEngine).execute(serviceContext, STMT_0_WITH_SCHEMA);
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_1);
    inOrder.verify(ksqlEngine).execute(serviceContext, STMT_1_WITH_SCHEMA);
  }

  @Test
  public void shouldTryExecuteStatementsReturnedByParserBeforeExecute() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(
        ImmutableList.of(PARSED_STMT_0, PARSED_STMT_1));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine, sandbox);
    inOrder.verify(sandbox).execute(sandbox.getServiceContext(), STMT_0_WITH_SCHEMA);
    inOrder.verify(sandbox).execute(sandbox.getServiceContext(), STMT_1_WITH_SCHEMA);
    inOrder.verify(ksqlEngine).execute(ksqlEngine.getServiceContext(), STMT_0_WITH_SCHEMA);
    inOrder.verify(ksqlEngine).execute(ksqlEngine.getServiceContext(), STMT_1_WITH_SCHEMA);
  }

  @Test
  public void shouldThrowIfParseFails() {
    // Given:
    when(ksqlEngine.parse(any()))
        .thenThrow(new KsqlException("Bad tings happen"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ksqlContext.sql("Some SQL", SOME_PROPERTIES)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Bad tings happen"));
  }

  @Test
  public void shouldThrowIfSandboxExecuteThrows() {
    // Given:
    when(sandbox.execute(any(), any(ConfiguredStatement.class)))
        .thenThrow(new KsqlException("Bad tings happen"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ksqlContext.sql("Some SQL", SOME_PROPERTIES)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Bad tings happen"));
  }

  @Test
  public void shouldThrowIfExecuteThrows() {
    // Given:
    when(ksqlEngine.execute(any(), any(ConfiguredStatement.class)))
        .thenThrow(new KsqlException("Bad tings happen"));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> ksqlContext.sql("Some SQL", SOME_PROPERTIES)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Bad tings happen"));
  }

  @Test
  public void shouldNotExecuteAnyStatementsIfTryExecuteThrows() {
    // Given:
    when(sandbox.execute(any(), any(ConfiguredStatement.class)))
        .thenThrow(new KsqlException("Bad tings happen"));

    // When:
    try {
      ksqlContext.sql("Some SQL", SOME_PROPERTIES);
    } catch (final KsqlException e) {
      // expected
    }

    // Then:
    verify(ksqlEngine, never()).execute(any(), any(ConfiguredStatement.class));
  }

  @Test
  public void shouldStartPersistentQueries() {
    // Given:
    when(ksqlEngine.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(persistentQuery).start();
  }

  @Test
  public void shouldNotBlowUpOnSqlThatDoesNotResultInPersistentQueries() {
    // Given:
    when(ksqlEngine.execute(any(), any(ConfiguredStatement.class)))
        .thenReturn(ExecuteResult.of(transientQuery));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    // Did not blow up.
  }

  @Test
  public void shouldCloseEngineBeforeServiceContextOnClose() {
    // When:
    ksqlContext.close();

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine, serviceContext);
    inOrder.verify(ksqlEngine).close();
    inOrder.verify(serviceContext).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferSchema() {
    // Given:
    when(schemaInjector.inject(any())).thenReturn((ConfiguredStatement) CFG_STMT_0);

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).execute(eq(serviceContext), eq(STMT_0_WITH_SCHEMA));
  }

  @Test
  public void shouldThrowIfFailedToInferSchema() {
    // Given:
    when(schemaInjector.inject(any()))
        .thenThrow(new RuntimeException("Boom"));

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> ksqlContext.sql("Some SQL", SOME_PROPERTIES)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Boom"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferTopic() {
    // Given:
    when(topicInjector.inject(any()))
        .thenReturn((ConfiguredStatement) STMT_0_WITH_TOPIC);

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).execute(eq(serviceContext), eq(STMT_0_WITH_TOPIC));
  }

  @Test
  public void shouldInferTopicWithValidArgs() {
    // Given:
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(topicInjector, times(2) /* once to validate, once to execute */)
        .inject(CFG_STMT_0);
  }

  @Test
  public void shouldThrowIfFailedToInferTopic() {
    // Given:
    when(topicInjector.inject(any()))
        .thenThrow(new RuntimeException("Boom"));

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> ksqlContext.sql("Some SQL", SOME_PROPERTIES)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Boom"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferTopicAfterInferringSchema() {
    // Given:
    when(schemaInjector.inject(any())).thenReturn((ConfiguredStatement) STMT_1_WITH_SCHEMA);
    when(topicInjector.inject(eq(CFG_STMT_1))).thenReturn((ConfiguredStatement) STMT_1_WITH_TOPIC);

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).execute(serviceContext, STMT_1_WITH_TOPIC);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldSetProperty() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0, PARSED_STMT_0));

    final PreparedStatement<SetProperty> set = PreparedStatement.of(
        "SET SOMETHING",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    );

    when(ksqlEngine.prepare(any()))
        .thenReturn((PreparedStatement) set)
        .thenReturn(PREPARED_STMT_0);

    // When:
    ksqlContext.sql("SQL;", ImmutableMap.of());

    // Then:
    verify(ksqlEngine).execute(
        serviceContext,
        ConfiguredStatement.of(PREPARED_STMT_0, SessionConfig.of(SOME_CONFIG, ImmutableMap.of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ))));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldSetPropertyOnlyOnCommandsFollowingTheSetStatement() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0, PARSED_STMT_0));

    final PreparedStatement<SetProperty> set = PreparedStatement.of(
        "SET SOMETHING",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    );

    when(ksqlEngine.prepare(any()))
        .thenReturn((PreparedStatement) PREPARED_STMT_0)
        .thenReturn(set);

    // When:
    ksqlContext.sql("SQL;", ImmutableMap.of());

    // Then:
    verify(ksqlEngine).execute(
        serviceContext,
        ConfiguredStatement.of(PREPARED_STMT_0, SessionConfig.of(SOME_CONFIG, ImmutableMap.of())
        ));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUnsetProperty() {
    // Given:
    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0, PARSED_STMT_0));

    final Map<String, Object> properties = ImmutableMap
        .of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final PreparedStatement<UnsetProperty> unset = PreparedStatement.of(
        "UNSET SOMETHING",
        new UnsetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    when(ksqlEngine.prepare(any()))
        .thenReturn((PreparedStatement) unset)
        .thenReturn(PREPARED_STMT_0);

    // When:
    ksqlContext.sql("SQL;", properties);

    // Then:
    verify(ksqlEngine).execute(
        serviceContext,
        ConfiguredStatement.of(PREPARED_STMT_0, SessionConfig.of(SOME_CONFIG, ImmutableMap.of())
        ));
  }
}
