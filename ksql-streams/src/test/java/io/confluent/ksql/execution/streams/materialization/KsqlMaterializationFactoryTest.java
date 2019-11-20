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

package io.confluent.ksql.execution.streams.materialization;

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
import io.confluent.ksql.execution.materialization.AggregatesInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.ProjectInfo;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.sqlpredicate.SqlPredicate;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory.AggregateMapperFactory;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory.MaterializationFactory;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory.SelectMapperFactory;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory.SqlPredicateFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class KsqlMaterializationFactoryTest {

  private static final Expression HAVING_EXP = mock(Expression.class);

  private static final LogicalSchema AGGREGATE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("KSQL_INTERNAL_COL_0"), SqlTypes.DOUBLE)
      .build();

  private static final LogicalSchema TABLE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("SUM"), SqlTypes.DOUBLE)
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
  private AggregatesInfo aggInfo;
  @Mock
  private SqlPredicateFactory sqlPredicateFactory;
  @Mock
  private ProcessingLogger filterProcessingLogger;
  @Mock
  private ProcessingLogger projectProcessingLogger;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private AggregateMapperFactory aggregateMapperFactory;
  @Mock
  private SelectMapperFactory selectMapperFactory;
  @Mock
  private SqlPredicate havingSqlPredicate;
  @Mock
  private Function<GenericRow, GenericRow> aggregateMapper;
  @Mock
  private Predicate<Struct, GenericRow> havingPredicate;
  @Mock
  private BiFunction<Object, GenericRow, GenericRow> selectMapper;
  @Mock
  private MaterializationFactory materializationFactory;
  @Mock
  private MaterializationInfo.AggregateMapInfo aggregateMapInfo;
  @Mock
  private ProjectInfo selectMapperInfo;
  @Mock
  private MaterializationInfo.SqlPredicateInfo sqlPredicateInfo;
  @Captor
  private ArgumentCaptor<List<BiFunction<Struct, GenericRow, Optional<GenericRow>>>> transforms;
  @Mock
  private Struct keyIn;
  @Mock
  private GenericRow rowIn;
  @Mock
  private GenericRow rowOut;

  private final QueryId queryId = new QueryId("start");
  private final Stacker contextStacker = new Stacker();

  private KsqlMaterializationFactory factory;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    factory = new KsqlMaterializationFactory(
        ksqlConfig,
        functionRegistry,
        processingLogContext,
        aggregateMapperFactory,
        sqlPredicateFactory,
        selectMapperFactory,
        materializationFactory
    );

    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger("start.filter")).thenReturn(filterProcessingLogger);
    when(processingLoggerFactory.getLogger("start.project")).thenReturn(projectProcessingLogger);

    when(aggregateMapInfo.visit(any())).thenCallRealMethod();
    when(selectMapperInfo.visit(any())).thenCallRealMethod();
    when(sqlPredicateInfo.visit(any())).thenCallRealMethod();

    when(aggregateMapInfo.getInfo()).thenReturn(aggInfo);
    when(selectMapperInfo.getSelectExpressions()).thenReturn(SELECTS);
    when(selectMapperInfo.getSchema()).thenReturn(AGGREGATE_SCHEMA);
    when(sqlPredicateInfo.getFilterExpression()).thenReturn(HAVING_EXP);
    when(sqlPredicateInfo.getSchema()).thenReturn(AGGREGATE_SCHEMA);

    when(aggregateMapperFactory.create(any(), any())).thenReturn(aggregateMapper);
    when(havingSqlPredicate.getPredicate()).thenReturn((Predicate) havingPredicate);
    when(sqlPredicateFactory.create(any(), any(), any(), any(), any()))
        .thenReturn(havingSqlPredicate);
    when(selectMapperFactory.create(any(), any(), any(), any(), any())).thenReturn(selectMapper);

    when(info.getTransforms()).thenReturn(
        ImmutableList.of(aggregateMapInfo, selectMapperInfo, sqlPredicateInfo));
    when(info.getSchema()).thenReturn(TABLE_SCHEMA);
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
    when(info.getTransforms()).thenReturn(ImmutableList.of(aggregateMapInfo, selectMapperInfo));

    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(sqlPredicateFactory, never()).create(any(), any(), any(), any(), any());
  }

  @Test
  public void shouldGetFilterProcessingLoggerWithCorrectParams() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(processingLoggerFactory).getLogger("start.filter");
  }

  @Test
  public void shouldBuildHavingPredicateWithCorrectParams() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

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
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(processingLoggerFactory).getLogger("start.project");
  }

  @Test
  public void shouldBuildSelectAggregateMapperWithCorrectParameters() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(aggregateMapperFactory).create(
        aggInfo,
        functionRegistry
    );
  }

  @Test
  public void shouldBuildSelectValueMapperWithCorrectParameters() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(selectMapperFactory).create(
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
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(materializationFactory).create(
        eq(materialization),
        eq(TABLE_SCHEMA),
        any()
    );
  }

  private BiFunction<Struct, GenericRow, Optional<GenericRow>> getTransform(final int index) {
    verify(materializationFactory).create(
        eq(materialization),
        eq(TABLE_SCHEMA),
        transforms.capture()
    );
    return transforms.getValue().get(index);
  }

  @Test
  public void shouldBuildMaterializationWithAggregateMapTransform() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);
    when(aggregateMapper.apply(any())).thenReturn(rowOut);

    // Then:
    final BiFunction<Struct, GenericRow, Optional<GenericRow>> transform = getTransform(0);
    assertThat(transform.apply(keyIn, rowIn).get(), is(rowOut));
    verify(aggregateMapper).apply(rowIn);
  }

  @Test
  public void shouldBuildMaterializationWithSelectTransform() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);
    when(selectMapper.apply(any(), any())).thenReturn(rowOut);

    // Then:
    final BiFunction<Struct, GenericRow, Optional<GenericRow>> transform = getTransform(1);
    assertThat(transform.apply(keyIn, rowIn).get(), is(rowOut));
    verify(selectMapper).apply(keyIn, rowIn);
  }

  @Test
  public void shouldBuildMaterializationWithSqlPredicateTransform() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);
    when(havingPredicate.test(any(), any())).thenReturn(false);

    // Then:
    final BiFunction<Struct, GenericRow, Optional<GenericRow>> transform = getTransform(2);
    assertThat(transform.apply(keyIn, rowIn), is(Optional.empty()));
    verify(havingPredicate).test(keyIn, rowIn);
  }

  @Test
  public void shouldReturnMaterialization() {
    // Given:
    final KsqlMaterialization ksqlMaterialization = mock(KsqlMaterialization.class);
    when(materializationFactory.create(any(), any(), any()))
        .thenReturn(ksqlMaterialization);

    // When:
    final Materialization result = factory
        .create(materialization, info, queryId, contextStacker);

    // Then:
    assertThat(result, is(ksqlMaterialization));
  }
}
