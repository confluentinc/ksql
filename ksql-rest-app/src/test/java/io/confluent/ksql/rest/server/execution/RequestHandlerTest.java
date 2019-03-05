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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.parser.ParserMatchers.preparedStatement;
import static io.confluent.ksql.parser.ParserMatchers.preparedStatementText;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestHandlerTest {

  private static final Duration DURATION_10_MS = Duration.ofMillis(10);

  @Mock KsqlEngine ksqlEngine;
  @Mock KsqlConfig ksqlConfig;
  @Mock ServiceContext serviceContext;
  @Mock CommandQueue commandQueue;
  @Mock
  DistributingExecutor distributor;
  @Mock KsqlEntity entity;

  private MetaStore metaStore;
  private RequestHandler handler;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    when(ksqlEngine.parse(any()))
        .thenAnswer(inv -> new DefaultKsqlParser().parse(inv.getArgument(0)));
    when(ksqlEngine.prepare(any()))
        .thenAnswer(invocation ->
            new DefaultKsqlParser().prepare(invocation.getArgument(0), metaStore));
    when(distributor.execute(any(), any(), any(), any(), any())).thenReturn(Optional.of(entity));
  }

  @Test
  public void shouldUseCustomExecutor() {
    // Given
    final KsqlEntity entity = mock(KsqlEntity.class);
    final StatementExecutor customExecutor = givenReturningExecutor(CreateStream.class, entity);
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor), clazz -> false);

    // When
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse("CREATE STREAM x WITH (kafka_topic='x');");
    final KsqlEntityList entities = handler.execute(statements, ImmutableMap.of());

    // Then
    assertThat(entities, contains(entity));
    verify(customExecutor, times(1))
        .execute(argThat(is(preparedStatement(instanceOf(CreateStream.class)))),
            eq(ksqlEngine),
            eq(serviceContext),
            eq(ksqlConfig),
            eq(ImmutableMap.of()));
    verify(commandQueue, never()).enqueueCommand(any(), any(), any());
  }

  @Test
  public void shouldDefaultToDistributor() {
    // Given
    givenRequestHandler(ImmutableMap.of(), clazz -> false);

    // When
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse("CREATE STREAM x WITH (kafka_topic='x');");
    final KsqlEntityList entities = handler.execute(statements, ImmutableMap.of());

    // Then
    assertThat(entities, contains(entity));
    verify(distributor, times(1))
        .execute(argThat(is(preparedStatement(instanceOf(CreateStream.class)))),
            eq(ksqlEngine),
            eq(serviceContext),
            eq(ksqlConfig),
            eq(ImmutableMap.of()));
  }

  @Test
  public void shouldDistributeProperties() {
    // Given
    givenRequestHandler(ImmutableMap.of(), clazz -> false);

    // When
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse("CREATE STREAM x WITH (kafka_topic='x');");
    final KsqlEntityList entities = handler.execute(statements, ImmutableMap.of("x", "y"));

    // Then
    assertThat(entities, contains(entity));
    verify(distributor, times(1))
        .execute(argThat(is(preparedStatement(instanceOf(CreateStream.class)))),
            eq(ksqlEngine),
            eq(serviceContext),
            eq(ksqlConfig),
            eq(ImmutableMap.of("x", "y")));
  }

  @Test
  public void shouldWaitForDistributedStatements() throws TimeoutException, InterruptedException {
    // Given
    StatementExecutor customExecutor = givenSequencedExecutor(CreateStream.class);
    givenRequestHandler(
        ImmutableMap.of(CreateStream.class, customExecutor),
        clazz -> !Explain.class.isAssignableFrom(clazz)
    );

    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse(
            // does not wait because it is first
            "CREATE STREAM x WITH (kafka_topic='x');"
                // waits for offset 1 but does not increment offset
                + "CREATE TABLE y WITH (kafka_topic='y', key='x');"
                // waits for offset 1 and increments offset
                + "CREATE STREAM y WITH (kafka_topic='y');"
                // waits for offset 2
                + "CREATE TABLE y WITH (kafka_topic='y', key='x');"
                // does not wait because it is blacklisted
                + "EXPLAIN x;"
        );

    // When
    handler.execute(statements, ImmutableMap.of());

    // Then
    verify(commandQueue, times(2)).ensureConsumedPast(1L, DURATION_10_MS);
    verify(commandQueue, times(1)).ensureConsumedPast(2L, DURATION_10_MS);
  }

  @Test
  public void shouldInlineRunScriptStatements() {
    // Given:
    final Map<String, Object> props = ImmutableMap.of(
        KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT,
        "CREATE STREAM X WITH (kafka_topic='x');");

    final StatementExecutor customExecutor = givenReturningExecutor(CreateStream.class, null);
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor), clazz -> false);

    // When:
    final List<ParsedStatement> statements = new DefaultKsqlParser()
        .parse("RUN SCRIPT '/some/script.sql';" );
    handler.execute(statements, props);

    // Then:
    verify(customExecutor, times(1))
        .execute(
            argThat(is(preparedStatementText("CREATE STREAM X WITH (kafka_topic='x');"))),
            eq(ksqlEngine),
            eq(serviceContext),
            eq(ksqlConfig),
            any());
  }

  private void givenRequestHandler(
      final Map<Class<? extends Statement>, StatementExecutor> executors,
      final Predicate<Class<? extends Statement>> mustSynchronize) {
    handler = new RequestHandler(
        executors,
        mustSynchronize,
        distributor,
        ksqlEngine,
        ksqlConfig,
        serviceContext,
        commandQueue,
        DURATION_10_MS
    );
  }

  private StatementExecutor givenReturningExecutor(
      final Class<? extends Statement> statementClass,
      final KsqlEntity entity
  ) {
    final StatementExecutor customExecutor = mock(StatementExecutor.class);
    when(customExecutor.execute(
        argThat(is(preparedStatement(instanceOf(statementClass)))),
        eq(ksqlEngine),
        eq(serviceContext),
        eq(ksqlConfig),
        any())).thenReturn(Optional.ofNullable(entity));
    return customExecutor;
  }

  private StatementExecutor givenSequencedExecutor(
      final Class<? extends Statement> statementClass
  ) {
    final AtomicLong scn = new AtomicLong();
    final StatementExecutor customExecutor = mock(StatementExecutor.class);
    when(customExecutor.execute(
        argThat(is(preparedStatement(instanceOf(statementClass)))),
        eq(ksqlEngine),
        eq(serviceContext),
        eq(ksqlConfig),
        any()))
        .thenAnswer(inv -> {
          final CommandStatusEntity commandStatusEntity = mock(CommandStatusEntity.class);
          when(commandStatusEntity.getCommandSequenceNumber()).thenReturn(scn.incrementAndGet());
          return Optional.of(commandStatusEntity);
        });
    return customExecutor;
  }


}
