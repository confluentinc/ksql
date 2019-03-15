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

import static io.confluent.ksql.parser.ParserMatchers.preparedStatement;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.Explain;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.inference.SchemaInjector;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestValidatorTest {

  private static final Schema SCHEMA =
      SchemaBuilder.struct().field("val", Schema.OPTIONAL_STRING_SCHEMA).build();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  KsqlEngine ksqlEngine;
  @Mock
  KsqlConfig ksqlConfig;
  @Mock
  StatementValidator<?> statementValidator;
  @Mock
  SchemaInjector schemaInjector;

  private ServiceContext serviceContext;
  private MutableMetaStore metaStore;
  private RequestValidator validator;
  private KsqlExecutionContext executionContext;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    when(ksqlEngine.parse(any()))
        .thenAnswer(inv -> new DefaultKsqlParser().parse(inv.getArgument(0)));
    when(ksqlEngine.prepare(any()))
        .thenAnswer(invocation ->
            new DefaultKsqlParser().prepare(invocation.getArgument(0), metaStore));
    executionContext = ksqlEngine;
    serviceContext = TestServiceContext.create();
    when(ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG))
        .thenReturn(Integer.MAX_VALUE);
    when(schemaInjector.forStatement(any())).thenAnswer(inv -> inv.getArgument(0));

    final KsqlStream<?> source = mock(KsqlStream.class);
    when(source.getName()).thenReturn("SOURCE");
    when(source.getSchema()).thenReturn(SCHEMA);

    final KsqlStream<?> sink = mock(KsqlStream.class);
    when(sink.getName()).thenReturn("SINK");
    when(sink.getSchema()).thenReturn(SCHEMA);

    metaStore.putSource(source);
    metaStore.putSource(sink);

    givenRequestValidator(ImmutableMap.of());
  }

  @Test
  public void shouldCallStatementValidator() {
    // Given:
    givenRequestValidator(
        ImmutableMap.of(CreateStream.class, statementValidator)
    );

    final List<ParsedStatement> statements =
        givenParsed("CREATE STREAM x WITH (kafka_topic='x');");

    // When:
    validator.validate(statements, ImmutableMap.of(), "sql");

    // Then:
    verify(statementValidator, times(1)).validate(
        argThat(is(preparedStatement(instanceOf(CreateStream.class)))),
        eq(executionContext),
        any(),
        eq(ksqlConfig),
        any()
    );
  }

  @Test
  public void shouldExecuteOnEngineIfNoCustomExecutor() {
    // Given:
    final List<ParsedStatement> statements = givenParsed("SET 'property'='value';");

    // When:
    validator.validate(statements, ImmutableMap.of(), "sql");

    // Then:
    verify(ksqlEngine, times(1)).execute(
        argThat(is(preparedStatement(instanceOf(SetProperty.class)))),
        eq(ksqlConfig),
        any());
  }

  @Test
  public void shouldThrowExceptionIfValidationFails() {
    // Given:
    givenRequestValidator(
        ImmutableMap.of(CreateStream.class, statementValidator)
    );
    doThrow(new KsqlException("Fail"))
        .when(statementValidator).validate(any(), any(), any(), any(), any());

    final List<ParsedStatement> statements =
        givenParsed("CREATE STREAM x WITH (kafka_topic='x');");

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Fail");

    // When:
    validator.validate(statements, ImmutableMap.of(), "sql");
  }

  @Test
  public void shouldThrowIfNoValidatorAvailable() {
    // Given:
    final List<ParsedStatement> statements =
        givenParsed("EXPLAIN X;");

    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Do not know how to validate statement");

    // When:
    validator.validate(statements, ImmutableMap.of(), "sql");
  }

  @Test
  public void shouldThrowIfTooManyPersistentQueries() {
    // Given:
    when(ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)).thenReturn(1);

    final List<ParsedStatement> statements =
        givenParsed(
            "CREATE STREAM sink AS SELECT * FROM source;"
                + "CREATE STREAM sink2 as SELECT * FROM sink;"
        );

    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("persistent queries to exceed the configured limit");

    // When:
    validator.validate(statements, ImmutableMap.of(), "sql");
  }

  @Test
  public void shouldNotThrowIfManyNonPersistentQueries() {
    // Given:
    givenRequestValidator(
        ImmutableMap.of(
            CreateStream.class, StatementValidator.NO_VALIDATION,
            Explain.class, StatementValidator.NO_VALIDATION)
    );
    when(ksqlConfig.getInt(KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG)).thenReturn(1);

    final List<ParsedStatement> statements =
        givenParsed(
            "CREATE STREAM a WITH (kafka_topic='a', value_format='json');"
                + "EXPLAIN x;"
        );

    // Expect Nothing:
    // When:
    validator.validate(statements, ImmutableMap.of(), "sql");
  }

  @Test
  public void shouldValidateRunScript() {
    // Given:
    final Map<String, Object> props = ImmutableMap.of(
        KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT,
        "CREATE STREAM x WITH (kafka_topic='x');");

    givenRequestValidator(
        ImmutableMap.of(CreateStream.class, statementValidator)
    );

    final List<ParsedStatement> statements = givenParsed("RUN SCRIPT '/some/script.sql';");

    // When:
    validator.validate(statements, props, "sql");

    // Then:
    verify(statementValidator, times(1)).validate(
        argThat(is(preparedStatement(instanceOf(CreateStream.class)))),
        eq(executionContext),
        any(),
        eq(ksqlConfig),
        any()
    );
  }

  private List<ParsedStatement> givenParsed(final String sql) {
    return new DefaultKsqlParser().parse(sql);
  }

  private void givenRequestValidator(
      Map<Class<? extends Statement>, StatementValidator<?>> customValidators
  ) {
    validator = new RequestValidator(
        customValidators,
        sc -> schemaInjector,
        () -> executionContext,
        serviceContext,
        ksqlConfig
    );
  }

}
