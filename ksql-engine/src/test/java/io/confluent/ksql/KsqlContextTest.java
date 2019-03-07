/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.schema.inference.TopicInjector;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.util.Collections;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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

  private final static PreparedStatement<?> STMT_0_WITH_SCHEMA = PreparedStatement
      .of("sql 0", mock(Statement.class));

  private final static PreparedStatement<?> STMT_1_WITH_SCHEMA = PreparedStatement
      .of("sql 1", mock(Statement.class));

  private final static PreparedStatement<?> STMT_0_WITH_TOPIC = PreparedStatement
      .of("sql 0", mock(Statement.class));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlExecutionContext sandbox;
  @Mock
  private PersistentQueryMetadata persistentQuery;
  @Mock
  private QueuedQueryMetadata transientQuery;
  @Mock
  private SchemaInjector schemaInjector;
  @Mock
  private Function<KsqlExecutionContext, TopicInjector> topicInjectorFactory;
  @Mock
  private TopicInjector topicInjector;

  private KsqlContext ksqlContext;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0));

    when(ksqlEngine.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(ksqlEngine.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(ksqlEngine.execute(any(), any(), any())).thenReturn(ExecuteResult.of("success"));

    when(ksqlEngine.createSandbox()).thenReturn(sandbox);

    when(sandbox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandbox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(schemaInjector.forStatement(PREPARED_STMT_0))
        .thenReturn((PreparedStatement) STMT_0_WITH_SCHEMA);
    when(schemaInjector.forStatement(PREPARED_STMT_1))
        .thenReturn((PreparedStatement) STMT_1_WITH_SCHEMA);

    when(topicInjectorFactory.apply(any())).thenReturn(topicInjector);
    when(topicInjector.forStatement(any(), any(), any()))
        .thenAnswer(inv -> inv.getArgument(0));

    ksqlContext = new KsqlContext(
        serviceContext, SOME_CONFIG, ksqlEngine, schemaInjector, topicInjectorFactory);

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
    inOrder.verify(ksqlEngine).execute(eq(STMT_0_WITH_SCHEMA), any(), any());
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_1);
    inOrder.verify(ksqlEngine).execute(eq(STMT_1_WITH_SCHEMA), any(), any());
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
    inOrder.verify(sandbox).execute(eq(STMT_0_WITH_SCHEMA), any(), any());
    inOrder.verify(sandbox).execute(eq(STMT_1_WITH_SCHEMA), any(), any());
    inOrder.verify(ksqlEngine).execute(eq(STMT_0_WITH_SCHEMA), any(), any());
    inOrder.verify(ksqlEngine).execute(eq(STMT_1_WITH_SCHEMA), any(), any());
  }

  @Test
  public void shouldThrowIfParseFails() {
    // Given:
    when(ksqlEngine.parse(any()))
        .thenThrow(new KsqlException("Bad tings happen"));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldThrowIfSandboxExecuteThrows() {
    // Given:
    when(sandbox.execute(any(), any(), any()))
        .thenThrow(new KsqlException("Bad tings happen"));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldThrowIfExecuteThrows() {
    // Given:
    when(ksqlEngine.execute(any(), any(), any()))
        .thenThrow(new KsqlException("Bad tings happen"));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldNotExecuteAnyStatementsIfTryExecuteThrows() {
    // Given:
    when(sandbox.execute(any(), any(), any()))
        .thenThrow(new KsqlException("Bad tings happen"));

    // When:
    try {
      ksqlContext.sql("Some SQL", SOME_PROPERTIES);
    } catch (final KsqlException e) {
      // expected
    }

    // Then:
    verify(ksqlEngine, never()).execute(any(), any(), any());
  }

  @Test
  public void shouldStartPersistentQueries() {
    // Given:
    when(ksqlEngine.execute(any(), any(), any()))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(persistentQuery).start();
  }

  @Test
  public void shouldNotBlowUpOnSqlThatDoesNotResultInPersistentQueries() {
    // Given:
    when(ksqlEngine.execute(any(), any(), any()))
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
    when(schemaInjector.forStatement(any())).thenReturn((PreparedStatement) STMT_0_WITH_SCHEMA);

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).execute(eq(STMT_0_WITH_SCHEMA), any(), any());
  }

  @Test
  public void shouldThrowIfFailedToInferSchema() {
    // Given:
    when(schemaInjector.forStatement(any()))
        .thenThrow(new RuntimeException("Boom"));

    // Then:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Boom");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferTopic() {
    // Given:
    when(topicInjector.forStatement(any(), any(), any()))
        .thenReturn((PreparedStatement) STMT_0_WITH_TOPIC);

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).execute(eq(STMT_0_WITH_TOPIC), any(), any());
  }

  @Test
  public void shouldThrowIfFailedToInferTopic() {
    // Given:
    when(topicInjector.forStatement(any(), any(), any()))
        .thenThrow(new RuntimeException("Boom"));

    // Then:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Boom");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }
}
