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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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

  private static final String AVRO_SCHEMA = SchemaBuilder.record("thing").fields()
      .name("thing1").type().optional().intType()
      .name("thing2").type().optional().stringType()
      .endRecord()
      .toString(true);

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
  private AbstractStreamCreateStatement createStatement;
  @Mock
  private AbstractStreamCreateStatement withSchema;
  @Mock
  private SchemaRegistryClient srClient;
  @Captor
  private ArgumentCaptor<PreparedStatement<?>> stmtCaptor;

  private KsqlContext ksqlContext;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    ksqlContext = new KsqlContext(serviceContext, SOME_CONFIG, ksqlEngine);

    when(ksqlEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0));

    when(ksqlEngine.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(ksqlEngine.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(ksqlEngine.execute(any(), any(), any())).thenReturn(ExecuteResult.of("success"));

    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);

    when(createStatement.copyWith(any(), any())).thenReturn(withSchema);

    when(createStatement.getProperties()).thenReturn(ImmutableMap.of(
        DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON")
    ));

    when(ksqlEngine.createSandbox()).thenReturn(sandbox);

    when(sandbox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandbox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);
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
    inOrder.verify(ksqlEngine).execute(eq(PREPARED_STMT_0), any(), any());
    inOrder.verify(ksqlEngine).prepare(PARSED_STMT_1);
    inOrder.verify(ksqlEngine).execute(eq(PREPARED_STMT_1), any(), any());
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
    inOrder.verify(sandbox).execute(eq(PREPARED_STMT_0), any(), any());
    inOrder.verify(sandbox).execute(eq(PREPARED_STMT_1), any(), any());
    inOrder.verify(ksqlEngine).execute(eq(PREPARED_STMT_0), any(), any());
    inOrder.verify(ksqlEngine).execute(eq(PREPARED_STMT_1), any(), any());
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

  @Test
  public void shouldLoadSchemaFromSchemaRegistry() throws Exception {
    // Given:
    givenAvroStatement();

    when(srClient.getLatestSchemaMetadata("topic-name-value"))
        .thenReturn(new SchemaMetadata(1, 1, AVRO_SCHEMA));

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(ksqlEngine).execute(stmtCaptor.capture(), any(), any());
    assertThat(stmtCaptor.getValue().getStatement(), is(withSchema));
  }

  @Test
  public void shouldThrowIfFailedToGetSchemaFromRegistry() throws Exception {
    // Given:
    givenAvroStatement();

    when(srClient.getLatestSchemaMetadata(any()))
        .thenThrow(new RestClientException("oops", 500, 344));

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema registry fetch for topic topic-name request failed");

    // When:
    ksqlContext.sql("Some SQL", SOME_PROPERTIES);
  }

  private void givenAvroStatement() {
    when(createStatement.getProperties()).thenReturn(ImmutableMap.of(
        DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("AVRO"),
        DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic-name")
    ));
  }
}
