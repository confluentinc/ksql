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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.ProjectInfo;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.Transform;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory.MaterializationFactory;
import io.confluent.ksql.execution.transform.KsqlValueTransformerWithKey;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
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

  private static final LogicalSchema TABLE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("SUM"), SqlTypes.DOUBLE)
      .build();

  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private Materialization materialization;
  @Mock
  private MaterializationInfo info;
  @Mock
  private KudafAggregator aggregator;
  @Mock
  private ProcessingLogger filterProcessingLogger;
  @Mock
  private ProcessingLogger projectProcessingLogger;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private ValueMapper<GenericRow, GenericRow> aggregateMapper;
  @Mock
  private Predicate<Object, GenericRow> havingPredicate;
  @Mock
  private KsqlValueTransformerWithKey<Object> selectTransformer;
  @Mock
  private MaterializationFactory materializationFactory;
  @Mock
  private MaterializationInfo.AggregateMapInfo aggregateMapInfo;
  @Mock
  private ProjectInfo projectInfo;
  @Mock
  private MaterializationInfo.SqlPredicateInfo sqlPredicateInfo;
  @Captor
  private ArgumentCaptor<List<Transform>> transforms;
  @Mock
  private Struct keyIn;
  @Mock
  private GenericRow rowIn;
  @Mock
  private GenericRow rowOut;

  private final QueryId queryId = new QueryId("start");
  private final Stacker contextStacker = new Stacker();

  private KsqlMaterializationFactory factory;

  @Before
  public void setUp() {
    factory = new KsqlMaterializationFactory(
        processingLogContext,
        materializationFactory
    );

    when(processingLogContext.getLoggerFactory()).thenReturn(processingLoggerFactory);
    when(processingLoggerFactory.getLogger("start.filter")).thenReturn(filterProcessingLogger);
    when(processingLoggerFactory.getLogger("start.project")).thenReturn(projectProcessingLogger);

    when(aggregateMapInfo.visit(any())).thenCallRealMethod();
    when(projectInfo.visit(any())).thenCallRealMethod();
    when(sqlPredicateInfo.visit(any())).thenCallRealMethod();

    when(aggregateMapInfo.getAggregator()).thenReturn(aggregator);
    when(projectInfo.getSelectTransformer(any())).thenReturn(selectTransformer);
    when(sqlPredicateInfo.getPredicate(any())).thenReturn(havingPredicate);

    when(aggregator.getResultMapper()).thenReturn(aggregateMapper);

    when(info.getTransforms())
        .thenReturn(ImmutableList.of(aggregateMapInfo, projectInfo, sqlPredicateInfo));

    when(info.getSchema()).thenReturn(TABLE_SCHEMA);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(ProcessingLogContext.class, processingLogContext)
        .testConstructors(KsqlMaterializationFactory.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldGetFilterProcessingLoggerWithCorrectParams() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(processingLoggerFactory).getLogger("start.filter");
  }

  @Test
  public void shouldUseCorrectLoggerForHavingPredicate() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(sqlPredicateInfo).getPredicate(filterProcessingLogger);
  }

  @Test
  public void shouldGetProjectProcessingLoggerWithCorrectParams() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(processingLoggerFactory).getLogger("start.project");
  }

  @Test
  public void shouldUseCorrectLoggerForSelectMapper() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);

    // Then:
    verify(projectInfo).getSelectTransformer(projectProcessingLogger);
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

  @Test
  public void shouldBuildMaterializationWithAggregateMapTransform() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);
    when(aggregateMapper.apply(any())).thenReturn(rowOut);

    // Then:
    final Transform transform = getTransform(0);
    assertThat(transform.apply(keyIn, rowIn).get(), is(rowOut));
  }

  @Test
  public void shouldBuildMaterializationWithSelectTransform() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);
    when(selectTransformer.transform(any(), any())).thenReturn(rowOut);

    // Then:
    final Transform transform = getTransform(1);
    assertThat(transform.apply(keyIn, rowIn).get(), is(rowOut));
  }

  @Test
  public void shouldBuildMaterializationWithSqlPredicateTransform() {
    // When:
    factory.create(materialization, info, queryId, contextStacker);
    when(havingPredicate.test(any(), any())).thenReturn(false);

    // Then:
    final Transform transform = getTransform(2);
    assertThat(transform.apply(keyIn, rowIn), is(Optional.empty()));
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

  private Transform getTransform(final int index) {
    verify(materializationFactory).create(
        eq(materialization),
        eq(TABLE_SCHEMA),
        transforms.capture()
    );
    return transforms.getValue().get(index);
  }
}