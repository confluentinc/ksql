/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushUtilTest {

  private static final Map<String, Object> overrides = ImmutableMap.of(
      "auto.offset.reset", "latest"
  );

  private static final Expression AN_EXPRESSION = mock(Expression.class);

  @Mock
  private Query query;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private SingleColumn singleColumn;
  @Mock
  private ColumnReferenceExp columnReferenceExp;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private Select select;

  @Test
  public void shouldNotMakeQueryWithRowpartitionInSelectClauseScalablePush() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      expectIsSPQ(SystemColumns.ROWPARTITION_NAME, columnExtractor);

      // When:
      final boolean isScalablePush = ScalablePushUtil.isScalablePushQuery(
          query,
          ksqlEngine,
          ksqlConfig,
          overrides
      );

      // Then:
      assert(!isScalablePush);
    }
  }

  @Test
  public void shouldNotMakeQueryWithRowoffsetInSelectClauseScalablePush() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      expectIsSPQ(SystemColumns.ROWOFFSET_NAME, columnExtractor);

      // When:
      final boolean isScalablePush = ScalablePushUtil.isScalablePushQuery(
          query,
          ksqlEngine,
          ksqlConfig,
          overrides
      );

      // Then:
      assert(!isScalablePush);
    }
  }

  @Test
  public void shouldNotMakeQueryWithRowpartitionInWhereClauseScalablePush() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      givenWhereClause(SystemColumns.ROWPARTITION_NAME, columnExtractor);

      // When:
      final boolean isScalablePush = ScalablePushUtil.isScalablePushQuery(
          query,
          ksqlEngine,
          ksqlConfig,
          overrides
      );

      // Then:
      assert(!isScalablePush);
    }
  }

  @Test
  public void shouldNotMakeQueryWithRowoffsetInWhereClauseScalablePush() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      givenWhereClause(SystemColumns.ROWOFFSET_NAME, columnExtractor);

      // When:
      final boolean isScalablePush = ScalablePushUtil.isScalablePushQuery(
          query,
          ksqlEngine,
          ksqlConfig,
          overrides
      );

      // Then:
      assert(!isScalablePush);
    }
  }

  @Test
  public void isScalablePushQuery_true() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);

      // When:
      final boolean isScalablePush = ScalablePushUtil.isScalablePushQuery(
          query,
          ksqlEngine,
          ksqlConfig,
          overrides
      );

      // Then:
      assert(isScalablePush);
    }
  }

  @Test
  public void isScalablePushQuery_false_configDisabled() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED)).thenReturn(false);

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_true_enabledWithOverride() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
              KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true)),
          equalTo(true));
    }
  }

  @Test
  public void isScalablePushQuery_false_hasGroupBy() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(query.getGroupBy())
          .thenReturn(
              Optional.of(new GroupBy(Optional.empty(), ImmutableList.of(new IntegerLiteral(1)))));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_false_hasWindow() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(query.getWindow()).thenReturn(Optional.of(new WindowExpression("foo",
          new TumblingWindowExpression(new WindowTimeClause(1, TimeUnit.MILLISECONDS)))));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_false_hasHaving() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(query.getHaving()).thenReturn(Optional.of(new IntegerLiteral(1)));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_false_hasPartitionBy() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(query.getPartitionBy())
          .thenReturn(Optional.of(
              new PartitionBy(Optional.empty(), ImmutableList.of(new IntegerLiteral(1)))));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_false_hasNoRefinement() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(query.getRefinement()).thenReturn(Optional.empty());

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_false_hasWrongRefinement() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(query.getRefinement()).thenReturn(Optional.of(RefinementInfo.of(OutputRefinement.FINAL)));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }


  @Test
  public void isScalablePushQuery_true_noLatest() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of()),
          equalTo(true));
    }
  }

  @Test
  public void isScalablePushQuery_true_configLatest() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(ksqlConfig.getKsqlStreamConfigProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
          .thenReturn(Optional.of("latest"));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of()),
          equalTo(true));
    }
  }

  @Test
  public void isScalablePushQuery_false_configNotLatest() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(ksqlConfig.getKsqlStreamConfigProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
          .thenReturn(Optional.of("earliest"));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of()),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_true_latestConfig() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(ksqlConfig.getKsqlStreamConfigProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
          .thenReturn(Optional.of("latest"));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of()),
          equalTo(true));
    }
  }

  @Test
  public void isScalablePushQuery_true_streamsOverride() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(
              KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(true));
    }
  }

  @Test
  public void isScalablePushQuery_false_wrongUpstreamQueries_None() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(ksqlEngine.getQueriesWithSink(SourceName.of("Foo"))).thenReturn(
          ImmutableSet.of());

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  @Test
  public void isScalablePushQuery_false_wrongUpstreamQueries_Two() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // When:
      expectIsSPQ(ColumnName.of("foo"), columnExtractor);
      when(ksqlEngine.getQueriesWithSink(SourceName.of("Foo"))).thenReturn(
          ImmutableSet.of(new QueryId("A"), new QueryId("B")));

      // Then:
      assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
          ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")),
          equalTo(false));
    }
  }

  private void expectIsSPQ(
      final ColumnName selectColumnName,
      final MockedStatic<ColumnExtractor> columnExtractor
  ) {
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED)).thenReturn(true);
    when(query.getGroupBy()).thenReturn(Optional.empty());
    when(query.getWindow()).thenReturn(Optional.empty());
    when(query.getHaving()).thenReturn(Optional.empty());
    when(query.getPartitionBy()).thenReturn(Optional.empty());
    when(query.getRefinement())
        .thenReturn(Optional.of(RefinementInfo.of(OutputRefinement.CHANGES)));
    when(query.getFrom())
        .thenReturn(new AliasedRelation(new Table(SourceName.of("Foo")), SourceName.of("blah")));
    when(ksqlEngine.getQueriesWithSink(SourceName.of("Foo"))).thenReturn(
        ImmutableSet.of(new QueryId("a")));
    givenSelectClause(selectColumnName, columnExtractor);
  }

  private void givenSelectClause(
      final ColumnName columnName,
      final MockedStatic<ColumnExtractor> columnExtractor
  ) {
    when(query.getSelect()).thenReturn(select);
    when(select.getSelectItems()).thenReturn(ImmutableList.of(singleColumn));
    when(singleColumn.getExpression()).thenReturn(AN_EXPRESSION);
    givenColumnExtraction(columnExtractor);
    when(columnReferenceExp.getColumnName()).thenReturn(columnName);
  }

  private void givenWhereClause(
      final ColumnName columnName,
      final MockedStatic<ColumnExtractor> columnExtractor
  ) {
    when(query.getWhere()).thenReturn(Optional.of(AN_EXPRESSION));
    givenColumnExtraction(columnExtractor);
    when(columnReferenceExp.getColumnName()).thenReturn(columnName);
  }

  private void givenColumnExtraction(
      MockedStatic<ColumnExtractor> columnExtractor) {
    columnExtractor.when(() -> ColumnExtractor.extractColumns(AN_EXPRESSION))
        .thenReturn(ImmutableSet.of(columnReferenceExp));
  }
}