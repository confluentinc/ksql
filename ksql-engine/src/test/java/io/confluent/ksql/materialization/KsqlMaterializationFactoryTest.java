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

package io.confluent.ksql.materialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.materialization.KsqlMaterializationFactory.MaterializationFactory;
import io.confluent.ksql.materialization.KsqlMaterializationFactory.SqlPredicateFactory;
import io.confluent.ksql.materialization.KsqlMaterializationFactory.ValueMapperFactory;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlMaterializationFactoryTest {

  private static final Expression HAVING_EXP = mock(Expression.class);

  private static final LogicalSchema AGGREGATE_SCHEMA = LogicalSchema.builder()
      .keyColumn("ROWKEY", SqlTypes.STRING)
      .valueColumn("KSQL_INTERNAL_COL_0", SqlTypes.DOUBLE)
      .build();

  private static final LogicalSchema TABLE_SCHEMA = LogicalSchema.builder()
      .keyColumn("ROWKEY", SqlTypes.STRING)
      .valueColumn("SUM", SqlTypes.DOUBLE)
      .build();

  private static final List<SelectExpression> SELECTS = ImmutableList.of(
      mock(SelectExpression.class)
  );

  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private Materialization materialization;
  @Mock
  private MaterializationInfo info;
  @Mock
  private SqlPredicateFactory sqlPredicateFactory;
  @Mock
  private ProcessingLogger filterProcessingLogger;
  @Mock
  private ProcessingLogger projectProcessingLogger;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ValueMapperFactory valueMapperFactory;
  @Mock
  private SqlPredicate havingSqlPredicate;
  @Mock
  private Predicate<Struct, GenericRow> havingPredicate;
  @Mock
  private Function<GenericRow, GenericRow> valueMapper;
  @Mock
  private MaterializationFactory materializationFactory;

  private final Stacker contextStacker = new Stacker(new QueryId("start"));

  private KsqlMaterializationFactory factory;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    factory = new KsqlMaterializationFactory(
        ksqlConfig,
        functionRegistry,
        processingLogContext,
        sqlPredicateFactory,
        valueMapperFactory,
        materializationFactory
    );

    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger("start.filter")).thenReturn(filterProcessingLogger);
    when(processingLoggerFactory.getLogger("start.project")).thenReturn(projectProcessingLogger);

    when(info.aggregationSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(info.tableSchema()).thenReturn(TABLE_SCHEMA);

    when(havingSqlPredicate.getPredicate()).thenReturn((Predicate) havingPredicate);
    when(sqlPredicateFactory.create(any(), any(), any(), any(), any()))
        .thenReturn(havingSqlPredicate);
    when(valueMapperFactory.create(any(), any(), any(), any(), any())).thenReturn(valueMapper);

    when(info.havingExpression()).thenReturn(Optional.of(HAVING_EXP));
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsqlConfig.class, ksqlConfig)
        .setDefault(FunctionRegistry.class, functionRegistry)
        .setDefault(ProcessingLogContext.class, processingLogContext)
        .testConstructors(KsqlMaterializationFactory.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldNotCreateSqlPredicateIfNoHavingClause() {
    // Given:
    when(info.havingExpression()).thenReturn(Optional.empty());

    // When:
    factory.create(materialization, info, contextStacker);

    // Then:
    verify(sqlPredicateFactory, never()).create(any(), any(), any(), any(), any());
  }

  @Test
  public void shouldGetFilterProcessingLoggerWithCorrectParams() {
    // When:
    factory.create(materialization, info, contextStacker);

    // Then:
    verify(processingLoggerFactory).getLogger("start.filter");
  }

  @Test
  public void shouldBuildHavingPredicateWithCorrectParams() {
    // When:
    factory.create(materialization, info, contextStacker);

    // Then:
    verify(sqlPredicateFactory).create(
        HAVING_EXP,
        AGGREGATE_SCHEMA,
        ksqlConfig,
        functionRegistry,
        filterProcessingLogger
    );
  }

  @Test
  public void shouldGetProjectProcessingLoggerWithCorrectParams() {
    // When:
    factory.create(materialization, info, contextStacker);

    // Then:
    verify(processingLoggerFactory).getLogger("start.project");
  }

  @Test
  public void shouldBuildSelectValueMapperWithCorrectParameters() {
    // Given:
    when(info.tableSelects()).thenReturn(SELECTS);

    // When:
    factory.create(materialization, info, contextStacker);

    // Then:
    verify(valueMapperFactory).create(
        SELECTS,
        AGGREGATE_SCHEMA,
        ksqlConfig,
        functionRegistry,
        projectProcessingLogger
    );
  }

  @Test
  public void shouldBuildMaterializationWithCorrectParams() {
    // When:
    factory.create(materialization, info, contextStacker);

    // Then:
    verify(materializationFactory).create(
        eq(materialization),
        eq(havingPredicate),
        any(),
        eq(TABLE_SCHEMA)
    );
  }

  @Test
  public void shouldReturnMaterialization() {
    // Given:
    final KsqlMaterialization ksqlMaterialization = mock(KsqlMaterialization.class);
    when(materializationFactory.create(any(), any(), any(), any())).thenReturn(ksqlMaterialization);

    // When:
    final Materialization result = factory
        .create(materialization, info, contextStacker);

    // Then:
    assertThat(result, is(ksqlMaterialization));
  }
}