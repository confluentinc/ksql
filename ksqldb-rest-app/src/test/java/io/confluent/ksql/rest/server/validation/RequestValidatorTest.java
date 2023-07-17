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

package io.confluent.ksql.rest.server.validation;

import static io.confluent.ksql.parser.ParserMatchers.configured;
import static io.confluent.ksql.parser.ParserMatchers.preparedStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.server.computation.ValidatedCommandFactory;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.statement.InjectorChain;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.Sandbox;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestValidatorTest {

  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();
  private static final String SOME_STREAM_SQL = "CREATE STREAM x WITH (value_format='json', kafka_topic='x');";

  @Mock
  private SandboxEngine sandboxEngine;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private StatementValidator<?> statementValidator;
  @Mock
  private Injector schemaInjector;
  @Mock
  private Injector topicInjector;
  @Mock
  private ValidatedCommandFactory distributedStatementValidator;
  @Mock
  private SessionProperties sessionProperties;

  private ServiceContext serviceContext;
  private MutableMetaStore metaStore;
  private RequestValidator validator;
  private KsqlExecutionContext executionContext;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    when(sandboxEngine.prepare(any(), any()))
        .thenAnswer(invocation ->
            KSQL_PARSER.prepare(invocation.getArgument(0), metaStore));
    when(sandboxEngine.getKsqlConfig()).thenReturn(ksqlConfig);
    executionContext = sandboxEngine;
    serviceContext = SandboxedServiceContext.create(TestServiceContext.create());
    when(ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG))
        .thenReturn(Integer.MAX_VALUE);
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));
    when(topicInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    final KsqlStream<?> source = mock(KsqlStream.class);
    when(source.getName()).thenReturn(SourceName.of("SOURCE"));

    final KsqlStream<?> sink = mock(KsqlStream.class);
    when(sink.getName()).thenReturn(SourceName.of("SINK"));

    metaStore.putSource(source, false);
    metaStore.putSource(sink, false);

    givenRequestValidator(ImmutableMap.of());
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldCallPrepareStatementWithSessionVariables() {
    // Given
    givenRequestValidator(ImmutableMap.of(CreateStream.class, statementValidator));
    final Map<String, String> sessionVariables = ImmutableMap.of("a", "1");
    when(sessionProperties.getSessionVariables()).thenReturn(sessionVariables);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE)).thenReturn(true);

    // When
    final List<ParsedStatement> statements = givenParsed(SOME_STREAM_SQL);
    validator.validate(serviceContext, statements, sessionProperties, "sql");

    // Then
    verify(sandboxEngine).prepare(statements.get(0), sessionVariables);
    verify(sessionProperties).getSessionVariables();
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldCallPrepareStatementWithEmptySessionVariablesIfSubstitutionDisabled() {
    // Given
    givenRequestValidator(ImmutableMap.of(CreateStream.class, statementValidator));
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE)).thenReturn(false);

    // When
    final List<ParsedStatement> statements = givenParsed(SOME_STREAM_SQL);
    validator.validate(serviceContext, statements, sessionProperties, "sql");

    // Then
    verify(sandboxEngine).prepare(statements.get(0), Collections.emptyMap());
    verify(sessionProperties, never()).getSessionVariables();
  }

  @Test
  public void shouldCallStatementValidator() {
    // Given:
    givenRequestValidator(
        ImmutableMap.of(CreateStream.class, statementValidator)
    );

    final List<ParsedStatement> statements =
        givenParsed(SOME_STREAM_SQL);

    // When:
    validator.validate(serviceContext, statements, sessionProperties, "sql");

    // Then:
    verify(statementValidator, times(1)).validate(
        argThat(is(configured(preparedStatement(instanceOf(CreateStream.class))))),
        eq(sessionProperties),
        eq(executionContext),
        any()
    );
  }

  @Test
  public void shouldExecuteOnDistributedStatementValidatorIfNoCustomExecutor() {
    // Given:
    final List<ParsedStatement> statements =
        givenParsed("CREATE STREAM foo WITH (kafka_topic='foo', value_format='json');");

    // When:
    validator.validate(serviceContext, statements, sessionProperties, "sql");

    // Then:
    verify(distributedStatementValidator).create(
        argThat(configured(preparedStatement(instanceOf(CreateStream.class)))),
        eq(serviceContext),
        eq(executionContext)
    );
  }

  @Test
  public void shouldThrowExceptionIfValidationFails() {
    // Given:
    givenRequestValidator(
        ImmutableMap.of(CreateStream.class, statementValidator)
    );
    doThrow(new KsqlException("Fail"))
        .when(statementValidator).validate(any(), any(), any(), any());

    final List<ParsedStatement> statements =
        givenParsed(SOME_STREAM_SQL);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(serviceContext, statements, sessionProperties, "sql")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Fail"));
  }

  @Test
  public void shouldThrowIfNoValidatorAvailable() {
    // Given:
    final List<ParsedStatement> statements =
        givenParsed("EXPLAIN X;");

    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> validator.validate(serviceContext, statements, sessionProperties, "sql")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Do not know how to validate statement"));
  }

  @Test
  public void shouldThrowIfTooManyPersistentQueries() {
    // Given:
    when(ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)).thenReturn(1);
    givenPersistentQueryCount(2);

    final List<ParsedStatement> statements =
        givenParsed(
            "CREATE STREAM sink AS SELECT * FROM source;"
                + "CREATE STREAM sink2 as SELECT * FROM sink;"
        );

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> validator.validate(serviceContext, statements, sessionProperties, "sql")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "persistent queries to exceed the configured limit"));
  }

  @Test
  public void shouldNotThrowIfNotQueryDespiteTooManyPersistentQueries() {
    // Given:
    givenPersistentQueryCount(2);
    givenRequestValidator(ImmutableMap.of(ListStreams.class, StatementValidator.NO_VALIDATION));

    final List<ParsedStatement> statements =
        givenParsed(
            "SHOW STREAMS;"
        );

    // When/Then:
    validator.validate(serviceContext, statements, sessionProperties, "sql");
  }

  @Test
  public void shouldNotThrowIfManyNonPersistentQueries() {
    // Given:
    givenRequestValidator(
        ImmutableMap.of(
            CreateStream.class, StatementValidator.NO_VALIDATION,
            Explain.class, StatementValidator.NO_VALIDATION)
    );

    final List<ParsedStatement> statements =
        givenParsed(
            "CREATE STREAM a WITH (kafka_topic='a', value_format='json');"
                + "EXPLAIN x;"
        );

    // Expect Nothing:
    // When:
    validator.validate(serviceContext, statements, sessionProperties, "sql");
  }

  @Test
  public void shouldThrowIfServiceContextIsNotSandbox() {
    // Given:
    serviceContext = mock(ServiceContext.class);
    givenRequestValidator(ImmutableMap.of());

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> validator.validate(serviceContext, ImmutableList.of(), sessionProperties, "sql")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected sandbox"));
  }

  @Test
  public void shouldThrowIfSnapshotSupplierReturnsNonSandbox() {
    // Given:
    executionContext = mock(KsqlExecutionContext.class);
    givenRequestValidator(ImmutableMap.of());

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> validator.validate(serviceContext, ImmutableList.of(), sessionProperties, "sql")
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Expected sandbox"));
  }

  @Test
  public void shouldExecuteWithSpecifiedServiceContext() {
    // Given:
    final List<ParsedStatement> statements = givenParsed(SOME_STREAM_SQL);
    final ServiceContext otherServiceContext =
        SandboxedServiceContext.create(TestServiceContext.create());

    // When:
    validator.validate(otherServiceContext, statements, sessionProperties, "sql");

    // Then:
    verify(distributedStatementValidator).create(
        argThat(configured(preparedStatement(instanceOf(CreateStream.class)))),
        same(otherServiceContext),
        any()
    );
  }

  private List<ParsedStatement> givenParsed(final String sql) {
    return KSQL_PARSER.parse(sql);
  }

  private void givenRequestValidator(
      final Map<Class<? extends Statement>, StatementValidator<?>> customValidators
  ) {
    validator = new RequestValidator(
        customValidators,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector),
        (sc) -> executionContext,
        distributedStatementValidator
    );
  }

  @SuppressWarnings("unchecked")
  private void givenPersistentQueryCount(final int value) {
    final List<PersistentQueryMetadata> queries = mock(List.class);
    when(queries.size()).thenReturn(value);
    when(sandboxEngine.getPersistentQueries()).thenReturn(queries);
  }

  @Sandbox
  private interface SandboxEngine extends KsqlExecutionContext {
  }

}
