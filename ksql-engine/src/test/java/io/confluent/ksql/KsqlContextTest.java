/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.util.Collections;
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
  private PreparedStatement<?> statement0;
  @Mock
  private PreparedStatement<?> statement1;
  private KsqlContext ksqlContext;

  @Before
  public void setUp() {
    ksqlContext = new KsqlContext(serviceContext, SOME_CONFIG, ksqlEngine);

    when(ksqlEngine.parseStatements(any()))
        .thenReturn(ImmutableList.of(statement0));

    when(ksqlEngine.createSandbox())
        .thenReturn(sandbox);

    when(ksqlEngine.execute(any(), any(), any())).thenReturn(ExecuteResult.of("success"));
  }

  @Test
  public void shouldParseStatements() {
    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).parseStatements("Some SQL");
  }

  @Test
  public void shouldTryExecuteStatementsReturnedByParser() {
    // Given:
    when(ksqlEngine.parseStatements(any()))
        .thenReturn(ImmutableList.of(statement0, statement1));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine)
        .tryExecute(ImmutableList.of(statement0, statement1), SOME_CONFIG, SOME_PROPERTIES);
  }

  @Test
  public void shouldTryExecuteStatementsReturnedByParserBeforeExecute() {
    // Given:
    when(ksqlEngine.parseStatements(any()))
        .thenReturn(ImmutableList.of(statement0, statement1));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine, sandbox);
    inOrder.verify(sandbox).execute(statement0, SOME_CONFIG, SOME_PROPERTIES);
    inOrder.verify(sandbox).execute(statement1, SOME_CONFIG, SOME_PROPERTIES);
    inOrder.verify(ksqlEngine).execute(statement0, SOME_CONFIG, SOME_PROPERTIES);
    inOrder.verify(ksqlEngine).execute(statement1, SOME_CONFIG, SOME_PROPERTIES);
  }

  @Test
  public void shouldThrowIfParseFails() {
    // Given:
    when(ksqlEngine.parseStatements(any()))
        .thenThrow(new KsqlException("Bad tings happen"));

    // Expect
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldThrowIfSanboxExecuteThrows() {
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
    when(ksqlEngine.tryExecute(any(), any(), any()))
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
}
