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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import io.confluent.ksql.util.KsqlException;
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

  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();
  private static final String SOME_STREAM_SQL = "CREATE STREAM x WITH (value_format='json', kafka_topic='x');";
  
  @Mock private KsqlEngine ksqlEngine;
  @Mock private KsqlConfig ksqlConfig;
  @Mock private ServiceContext serviceContext;
  @Mock private DistributingExecutor distributor;
  @Mock private KsqlEntity entity;
  @Mock private CommandQueueSync sync;
  @Mock private SessionProperties sessionProperties;
  @Mock private StatementExecutorResponse response;

  private MetaStore metaStore;
  private RequestHandler handler;
  private KsqlSecurityContext securityContext;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    when(ksqlEngine.prepare(any(), any()))
        .thenAnswer(invocation ->
            KSQL_PARSER.prepare(invocation.getArgument(0), metaStore));
    when(distributor.execute(any(), any(), any())).thenReturn(response);
    when(response.getEntity()).thenReturn(Optional.of(entity));
    when(sessionProperties.getMutableScopedProperties()).thenReturn(ImmutableMap.of());
    when(ksqlEngine.getKsqlConfig()).thenReturn(ksqlConfig);
    doNothing().when(sync).waitFor(any(), any());

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);
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
        KSQL_PARSER.parse(SOME_STREAM_SQL);
    final KsqlEntityList entities = handler.execute(securityContext, statements, sessionProperties);

    // Then
    assertThat(entities, contains(entity));
    verify(customExecutor, times(1))
        .execute(argThat(is(configured(
            preparedStatement(instanceOf(CreateStream.class)),
            ImmutableMap.of(),
            ksqlConfig))),
            eq(sessionProperties),
            eq(ksqlEngine),
            eq(serviceContext)
        );
  }

  @Test
  public void shouldThrowOnCreateStreamIfFeatureFlagIsDisabled() {
    // Given
    final StatementExecutor<CreateStream> customExecutor =
        givenReturningExecutor(CreateStream.class, mock(KsqlEntity.class));
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED)).thenReturn(false);
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor));

    // When
    final List<ParsedStatement> statements =
        KSQL_PARSER.parse("CREATE STREAM x (c1 ARRAY<STRUCT<`KEY` STRING, `VALUE` BYTES>> HEADERS) "
            + "WITH (value_format='json', kafka_topic='x');");
    final Exception e = assertThrows(
        KsqlException.class,
        () -> handler.execute(securityContext, statements, sessionProperties));

    // Then
    assertThat(e.getMessage(), containsString(
        "Cannot create Stream because schema with headers columns is disabled."));
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldCallPrepareStatementWithSessionVariables() {
    // Given
    final StatementExecutor<CreateStream> customExecutor =
        givenReturningExecutor(CreateStream.class, mock(KsqlEntity.class));
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor));
    final Map<String, String> sessionVariables = ImmutableMap.of("a", "1");
    when(sessionProperties.getSessionVariables()).thenReturn(sessionVariables);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE)).thenReturn(true);

    // When
    final List<ParsedStatement> statements = KSQL_PARSER.parse(SOME_STREAM_SQL);
    handler.execute(securityContext, statements, sessionProperties);

    // Then
    verify(ksqlEngine).prepare(statements.get(0), sessionVariables);
    verify(sessionProperties).getSessionVariables();
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldCallPrepareStatementWithEmptySessionVariablesIfSubstitutionDisabled() {
    // Given
    final StatementExecutor<CreateStream> customExecutor =
        givenReturningExecutor(CreateStream.class, mock(KsqlEntity.class));
    givenRequestHandler(ImmutableMap.of(CreateStream.class, customExecutor));
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE)).thenReturn(false);

    // When
    final List<ParsedStatement> statements = KSQL_PARSER.parse(SOME_STREAM_SQL);
    handler.execute(securityContext, statements, sessionProperties);

    // Then
    verify(ksqlEngine).prepare(statements.get(0), Collections.emptyMap());
    verify(sessionProperties, never()).getSessionVariables();
  }

  @Test
  public void shouldDefaultToDistributor() {
    // Given
    givenRequestHandler(ImmutableMap.of());

    // When
    final List<ParsedStatement> statements = KSQL_PARSER.parse(SOME_STREAM_SQL);
    final KsqlEntityList entities = handler.execute(securityContext, statements, sessionProperties);

    // Then
    assertThat(entities, contains(entity));
    verify(distributor, times(2))
        .execute(argThat(is(configured(
            preparedStatement(instanceOf(CreateStream.class)),
            ImmutableMap.of(),
            ksqlConfig))),
            eq(ksqlEngine),
            eq(securityContext)
        );
  }

  @Test
  public void shouldDistributeProperties() {
    // Given
    givenRequestHandler(ImmutableMap.of());
    when(sessionProperties.getMutableScopedProperties()).thenReturn(ImmutableMap.of("x", "y"));
    // When
    final List<ParsedStatement> statements =
        KSQL_PARSER.parse(SOME_STREAM_SQL);
    final KsqlEntityList entities = handler.execute(
        securityContext,
        statements,
        sessionProperties
    );

    // Then
    assertThat(entities, contains(entity));
    verify(distributor, times(2))
        .execute(
            argThat(is(configured(
                preparedStatement(instanceOf(CreateStream.class)),
                    ImmutableMap.of("x", "y"),
                    ksqlConfig))),
            eq(ksqlEngine),
            eq(securityContext)
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
        KSQL_PARSER.parse(
            "CREATE STREAM x WITH (value_format='json', kafka_topic='x');"
                + "CREATE STREAM y WITH (value_format='json', kafka_topic='y');"
                + "CREATE STREAM z WITH (value_format='json', kafka_topic='z');"
        );

    // When
    handler.execute(securityContext, statements, sessionProperties);

    // Then
    verify(sync).waitFor(argThat(hasItems(entity1, entity2)), any());
    // since the entities passed into sync#waitFor are always the same object, mockito
    // cannot verify the original two arguments
    verify(sync, times(3)).waitFor(any(), any());
  }

  private void givenRequestHandler(
      final Map<Class<? extends Statement>, StatementExecutor<?>> executors) {
    handler = new RequestHandler(
        executors,
        distributor,
        ksqlEngine,
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
        .thenAnswer(inv -> StatementExecutorResponse.handled(Optional.ofNullable(returnedEntities[scn.getAndIncrement()])));
    return customExecutor;
  }

  private static Matcher<KsqlEntityList> hasItems(final KsqlEntity... items) {
    return new TypeSafeMatcher<KsqlEntityList>() {
      @Override
      protected boolean matchesSafely(final KsqlEntityList actual) {
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
      public void describeTo(final Description description) {
        description.appendText(Arrays.toString(items));
      }
    };
  }
}
