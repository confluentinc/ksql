package io.confluent.ksql.rest.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ColumnExtractor;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Select;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushUtilTest {

  private static final Map<String, Object> overrides = ImmutableMap.of(
      KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true,
      KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED, true,
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
  private QueryId queryId;
  @Mock
  private RefinementInfo refinementInfo;
  @Mock
  private Select select;

  private Table table;
  private Set<QueryId> queries;

  @Before
  public void setup() {
    queries = ImmutableSet.of(queryId);
    table = new Table(Optional.empty(), SourceName.of("asdf"));
  }

  @Test
  public void shouldNotMakeQueryWithRowpartitionInSelectClauseScalablePush() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      givenScalablePushQuery();
      givenSelectClause(SystemColumns.ROWPARTITION_NAME, columnExtractor);

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
      givenScalablePushQuery();
      givenSelectClause(SystemColumns.ROWOFFSET_NAME, columnExtractor);

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
      givenScalablePushQuery();
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
      givenScalablePushQuery();
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
  public void shouldMakeCompatibleQueryScalablePush() {
    try(MockedStatic<ColumnExtractor> columnExtractor = mockStatic(ColumnExtractor.class)) {
      // Given:
      givenScalablePushQuery();
      givenSelectClause(ColumnName.of("AnAllowedColumnName"), columnExtractor);

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

  private void givenScalablePushQuery() {
    when(query.getFrom()).thenReturn(table);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED)).thenReturn(true);
    when(ksqlEngine.getQueriesWithSink(any())).thenReturn(queries);
    when(query.isPullQuery()).thenReturn(false);
    when(query.getGroupBy()).thenReturn(Optional.empty());
    when(query.getWindow()).thenReturn(Optional.empty());
    when(query.getHaving()).thenReturn(Optional.empty());
    when(query.getPartitionBy()).thenReturn(Optional.empty());
    when(query.getRefinement()).thenReturn(Optional.of(refinementInfo));
    when(refinementInfo.getOutputRefinement()).thenReturn(OutputRefinement.CHANGES);
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