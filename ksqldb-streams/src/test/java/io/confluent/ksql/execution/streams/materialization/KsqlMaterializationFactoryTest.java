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
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.MapperInfo;
import io.confluent.ksql.execution.materialization.MaterializationInfo.PredicateInfo;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.Transform;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterializationFactory.MaterializationFactory;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.ProcessingLoggerFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class KsqlMaterializationFactoryTest {

  private static final LogicalSchema TABLE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("SUM"), SqlTypes.DOUBLE)
      .build();

  @Mock
  private ProcessingLogContext processingLogContext;
  @Mock
  private StreamsMaterialization materialization;
  @Mock
  private MaterializationInfo info;
  @Mock
  private ProcessingLogger filterProcessingLogger;
  @Mock
  private ProcessingLogger mapProcessingLogger;
  @Mock
  private ProcessingLoggerFactory processingLoggerFactory;
  @Mock
  private KsqlTransformer<Object, GenericRow> mapper;
  @Mock
  private KsqlTransformer<Object, Optional<GenericRow>> predicate;
  @Mock
  private MaterializationFactory materializationFactory;
  @Mock
  private MapperInfo mapperInfo;
  @Mock
  private PredicateInfo predicateInfo;
  @Captor
  private ArgumentCaptor<List<Transform>> transforms;
  @Mock
  private Struct keyIn;
  @Mock
  private GenericRow rowIn;
  @Mock
  private GenericRow rowOut;
  @Mock
  private KsqlProcessingContext ctx;
  @Captor
  private ArgumentCaptor<Function<QueryContext, ProcessingLogger>> loggerCaptor;

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
    when(processingLoggerFactory.getLogger("pull_query.filter")).thenReturn(filterProcessingLogger);
    when(processingLoggerFactory.getLogger("pull_query.project")).thenReturn(mapProcessingLogger);

    when(mapperInfo.visit(any())).thenCallRealMethod();
    when(predicateInfo.visit(any())).thenCallRealMethod();

    when(mapperInfo.getMapper(any())).thenReturn(mapper);
    when(predicateInfo.getPredicate(any())).thenReturn(predicate);

    when(info.getTransforms())
        .thenReturn(ImmutableList.of(mapperInfo, predicateInfo));

    when(info.getSchema()).thenReturn(TABLE_SCHEMA);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(ProcessingLogContext.class, processingLogContext)
        .testConstructors(KsqlMaterializationFactory.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldUseCorrectLoggerForPredicate() {
    // When:
    factory.create(materialization, info, queryId, new Stacker().push("filter"));

    // Then:
    verify(predicateInfo).getPredicate(loggerCaptor.capture());
    assertThat(
        loggerCaptor.getValue().apply(new Stacker().getQueryContext()),
        is(filterProcessingLogger)
    );
  }

  @Test
  public void shouldUseCorrectLoggerForSelectMapper() {
    // When:
    factory.create(materialization, info, queryId, new Stacker().push("project"));

    // Then:
    verify(mapperInfo).getMapper(loggerCaptor.capture());
    assertThat(
        loggerCaptor.getValue().apply(new Stacker().getQueryContext()),
        is(mapProcessingLogger)
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

  @Test
  public void shouldBuildMaterializationWithMapTransform() {
    // Given:
    factory.create(materialization, info, queryId, contextStacker);
    when(mapper.transform(any(), any(), any())).thenReturn(rowOut);

    final Transform transform = getTransform(0);

    // When:
    final Optional<GenericRow> result = transform.apply(keyIn, rowIn, ctx);

    // Then:
    assertThat(result, is(Optional.of(rowOut)));
  }

  @Test
  public void shouldBuildMaterializationWithNegativePredicateTransform() {
    // Given:
    factory.create(materialization, info, queryId, contextStacker);
    when(predicate.transform(any(), any(), any())).thenReturn(Optional.empty());

    final Transform transform = getTransform(1);

    // Then:
    final Optional<GenericRow> result = transform.apply(keyIn, rowIn, ctx);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldBuildMaterializationWithPositivePredicateTransform() {
    // Given:
    factory.create(materialization, info, queryId, contextStacker);
    when(predicate.transform(any(), any(), any()))
        .thenAnswer(inv -> Optional.of(inv.getArgument(1)));

    final Transform transform = getTransform(1);

    // Then:
    final Optional<GenericRow> result = transform.apply(keyIn, rowIn, ctx);

    // Then:
    assertThat(result, is(Optional.of(rowIn)));
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
