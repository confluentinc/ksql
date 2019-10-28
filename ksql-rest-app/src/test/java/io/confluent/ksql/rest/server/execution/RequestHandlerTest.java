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

package io.confluent.ksql.rest.server.execution;

import static io.confluent.ksql.parser.ParserMatchers.configured;
import static io.confluent.ksql.parser.ParserMatchers.preparedStatement;
import static io.confluent.ksql.parser.ParserMatchers.preparedStatementText;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
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
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.TransactionalProducer;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestHandlerTest {

  private static final String SOME_STREAM_SQL = "CREATE STREAM x WITH (value_format='json', kafka_topic='x');";

  @Mock KsqlEngine ksqlEngine;
  @Mock KsqlConfig ksqlConfig;
  @Mock ServiceContext serviceContext;
  @Mock DistributingExecutor distributor;
  @Mock KsqlEntity entity;
  @Mock CommandQueueSync sync;
  @Mock
  TransactionalProducer transactionalProducer;

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
    when(distributor.execute(any(), any(), any(), any())).thenReturn(Optional.of(entity));
    doNothing().when(sync).waitFor(any(), any());
  }

  @Test
  public void shouldUseCustomExecutor() {
    // Given
    final KsqlEntity entity = mock(KsqlEntity.class);
    final StatementExecutor<CreateStream> customExecutor =
        givenReturningExecutor(CreateStream.class, entity);
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor));

    // When
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse(SOME_STREAM_SQL);
    final KsqlEntityList entities = handler.execute(serviceContext, statements, ImmutableMap.of(), transactionalProducer);

    // Then
    assertThat(entities, contains(entity));
    verify(customExecutor, times(1))
        .execute(argThat(is(configured(
            preparedStatement(instanceOf(CreateStream.class)),
            ImmutableMap.of(),
            ksqlConfig))),
            eq(ImmutableMap.of()),
            eq(ksqlEngine),
            eq(serviceContext)
        );
  }

  @Test
  public void shouldDefaultToDistributor() {
    // Given
    givenRequestHandler(ImmutableMap.of());

    // When
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse(SOME_STREAM_SQL);
    final KsqlEntityList entities = handler.execute(serviceContext, statements, ImmutableMap.of(), transactionalProducer);

    // Then
    assertThat(entities, contains(entity));
    verify(distributor, times(1))
        .execute(argThat(is(configured(
            preparedStatement(instanceOf(CreateStream.class)),
            ImmutableMap.of(),
            ksqlConfig))),
            eq(ImmutableMap.of()),
            eq(ksqlEngine),
            eq(serviceContext)
        );
  }

  @Test
  public void shouldDistributeProperties() {
    // Given
    givenRequestHandler(ImmutableMap.of());

    // When
    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse(SOME_STREAM_SQL);
    final KsqlEntityList entities = handler.execute(
        serviceContext,
        statements,
        ImmutableMap.of("x", "y"),
            transactionalProducer
    );

    // Then
    assertThat(entities, contains(entity));
    verify(distributor, times(1))
        .execute(argThat(is(configured(
            preparedStatement(instanceOf(CreateStream.class)),
            ImmutableMap.of("x", "y"),
            ksqlConfig))),
            any(),
            eq(ksqlEngine),
            eq(serviceContext)
        );
  }

  @Test
  public void shouldWaitForDistributedStatements() {
    // Given
    final KsqlEntity entity1 = mock(KsqlEntity.class);
    final KsqlEntity entity2 = mock(KsqlEntity.class);
    final KsqlEntity entity3 = mock(KsqlEntity.class);

    final StatementExecutor<CreateStream> customExecutor = givenReturningExecutor(
        CreateStream.class, entity1, entity2, entity3);
    givenRequestHandler(
        ImmutableMap.of(CreateStream.class, customExecutor)
    );

    final List<ParsedStatement> statements =
        new DefaultKsqlParser().parse(
            "CREATE STREAM x WITH (value_format='json', kafka_topic='x');"
                + "CREATE STREAM y WITH (value_format='json', kafka_topic='y');"
                + "CREATE STREAM z WITH (value_format='json', kafka_topic='z');"
        );

    // When
    handler.execute(serviceContext, statements, ImmutableMap.of(), transactionalProducer);

    // Then
    verify(sync).waitFor(argThat(hasItems(entity1, entity2)), any());
    // since the entities passed into sync#waitFor are always the same object, mockito
    // cannot verify the original two arguments
    verify(sync, times(3)).waitFor(any(), any());
  }

  @Test
  public void shouldInlineRunScriptStatements() {
    // Given:
    final Map<String, Object> props = ImmutableMap.of(
        KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT,
        SOME_STREAM_SQL);

    final StatementExecutor<CreateStream> customExecutor = givenReturningExecutor(
        CreateStream.class,
        (KsqlEntity) null);
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor));

    // When:
    final List<ParsedStatement> statements = new DefaultKsqlParser()
        .parse("RUN SCRIPT '/some/script.sql';" );
    handler.execute(serviceContext, statements, props, transactionalProducer);

    // Then:
    verify(customExecutor, times(1))
        .execute(
            argThat(is(configured(preparedStatementText(SOME_STREAM_SQL)))),
            any(),
            eq(ksqlEngine),
            eq(serviceContext)
        );
  }

  @Test
  public void shouldOnlyReturnLastInRunScript() {
    // Given:
    final KsqlEntity entity1 = mock(KsqlEntity.class);
    final KsqlEntity entity2 = mock(KsqlEntity.class);

    final Map<String, Object> props = ImmutableMap.of(
        KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT,
            SOME_STREAM_SQL
            + "CREATE STREAM Y WITH (value_format='json', kafka_topic='y');");

    final StatementExecutor<CreateStream> customExecutor = givenReturningExecutor(
        CreateStream.class, entity1, entity2);
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor));

    final List<ParsedStatement> statements = new DefaultKsqlParser()
        .parse("RUN SCRIPT '/some/script.sql';" );

    // When:
    final KsqlEntityList result = handler.execute(serviceContext, statements, props, transactionalProducer);

    // Then:
    assertThat(result, contains(entity2));
  }

  private void givenRequestHandler(
      final Map<Class<? extends Statement>, StatementExecutor<?>> executors) {
    handler = new RequestHandler(
        executors,
        distributor,
        ksqlEngine,
        ksqlConfig,
        sync
    );
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> StatementExecutor<T> givenReturningExecutor(
      final Class<T> statementClass,
      final KsqlEntity... returnedEntities
  ) {
    final AtomicInteger scn = new AtomicInteger();
    final StatementExecutor<T> customExecutor = mock(StatementExecutor.class);
    when(customExecutor.execute(
        argThat(is(configured(preparedStatement(instanceOf(statementClass))))),
        any(),
        eq(ksqlEngine),
        eq(serviceContext)
    ))
        .thenAnswer(inv -> Optional.ofNullable(returnedEntities[scn.getAndIncrement()]));
    return customExecutor;
  }

  private static Matcher<KsqlEntityList> hasItems(final KsqlEntity... items) {
    return new TypeSafeMatcher<KsqlEntityList>() {
      @Override
      protected boolean matchesSafely(KsqlEntityList actual) {
        if (items.length != actual.size()) {
          return false;
        }

        for (int i = 0; i < actual.size(); i++) {
          if (!actual.get(i).equals(items[i])) {
            return false;
          }
        }
        return true;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(Arrays.toString(items));
      }
    };
  }
}
