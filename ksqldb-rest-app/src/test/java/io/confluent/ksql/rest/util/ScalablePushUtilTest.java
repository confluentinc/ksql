package io.confluent.ksql.rest.util;

import static io.confluent.ksql.rest.util.ScalablePushUtil.STREAMS_AUTO_OFFSET_RESET_CONFIG;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalablePushUtilTest {

  @Mock
  private Query query;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private KsqlConfig ksqlConfig;

  private void expectIsSQP() {
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
  }

  @Test
  public void isScalablePushQuery_true() {
    // When:
    expectIsSQP();

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(true));
  }

  @Test
  public void isScalablePushQuery_false_configDisabled() {
    // When:
    expectIsSQP();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED)).thenReturn(false);

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }


  @Test
  public void isScalablePushQuery_true_enabledWithOverride() {
    // When:
    expectIsSQP();
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED)).thenReturn(false);

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest",
            KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true)),
        equalTo(true));
  }

  @Test
  public void isScalablePushQuery_false_hasGroupBy() {
    // When:
    expectIsSQP();
    when(query.getGroupBy())
        .thenReturn(
            Optional.of(new GroupBy(Optional.empty(), ImmutableList.of(new IntegerLiteral(1)))));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_false_hasWindow() {
    // When:
    expectIsSQP();
    when(query.getWindow()).thenReturn(Optional.of(new WindowExpression("foo",
        new TumblingWindowExpression(new WindowTimeClause(1, TimeUnit.MILLISECONDS)))));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_false_hasHaving() {
    // When:
    expectIsSQP();
    when(query.getHaving()).thenReturn(Optional.of(new IntegerLiteral(1)));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_false_hasPartitionBy() {
    // When:
    expectIsSQP();
    when(query.getPartitionBy())
        .thenReturn(Optional.of(
            new PartitionBy(Optional.empty(), ImmutableList.of(new IntegerLiteral(1)))));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_false_hasNoRefinement() {
    // When:
    expectIsSQP();
    when(query.getRefinement()).thenReturn(Optional.empty());

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_false_hasWrongRefinement() {
    // When:
    expectIsSQP();
    when(query.getRefinement()).thenReturn(Optional.of(RefinementInfo.of(OutputRefinement.FINAL)));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }


  @Test
  public void isScalablePushQuery_false_noLatest() {
    // When:
    expectIsSQP();

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of()),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_true_latestConfig() {
    // When:
    expectIsSQP();
    when(ksqlConfig.getKsqlStreamConfigProp(STREAMS_AUTO_OFFSET_RESET_CONFIG))
        .thenReturn(Optional.of("latest"));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of()),
        equalTo(true));
  }

  @Test
  public void isScalablePushQuery_true_streamsOverride() {
    // When:
    expectIsSQP();

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(
            KsqlConfig.KSQL_STREAMS_PREFIX + STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(true));
  }

  @Test
  public void isScalablePushQuery_false_wrongUpstreamQueries_None() {
    // When:
    expectIsSQP();
    when(ksqlEngine.getQueriesWithSink(SourceName.of("Foo"))).thenReturn(
        ImmutableSet.of());

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }

  @Test
  public void isScalablePushQuery_false_wrongUpstreamQueries_Two() {
    // When:
    expectIsSQP();
    when(ksqlEngine.getQueriesWithSink(SourceName.of("Foo"))).thenReturn(
        ImmutableSet.of(new QueryId("A"), new QueryId("B")));

    // Then:
    assertThat(ScalablePushUtil.isScalablePushQuery(query, ksqlEngine, ksqlConfig,
        ImmutableMap.of(STREAMS_AUTO_OFFSET_RESET_CONFIG, "latest")),
        equalTo(false));
  }
}
