package io.confluent.ksql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.streams.RoutingOptions;
import io.confluent.ksql.internal.PullQueryExecutorMetrics;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.PullPhysicalPlan;
import io.confluent.ksql.physical.pull.PullQueryResult;
import io.confluent.ksql.planner.LogicalPlanNode;
import io.confluent.ksql.planner.QueryPlannerOptions;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EngineExecutorTest {

  @Mock
  private EngineContext engineContext;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ConfiguredStatement<Query> statement;
  @Mock
  private Query query;
  @Mock
  private HARouting haRouting;
  @Mock
  private RoutingOptions routingOptions;
  @Mock
  private QueryPlannerOptions queryPlannerOptions;
  @Mock
  private PullQueryExecutorMetrics pullQueryMetrics;
  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private ImmutableAnalysis immutableAnalysis;
  @Mock
  private LogicalPlanNode logicalPlanNode;
  @Mock
  private PullPhysicalPlan pullPhysicalPlan;


  private EngineExecutor engineExecutor;

  @Before
  public void setUp() {
    final SessionConfig sessionConfig = SessionConfig.of(ksqlConfig, Collections.emptyMap());
    when(statement.getStatement()).thenReturn(query);
    when(query.isPullQuery()).thenReturn(true);
    when(statement.getStatementText()).thenReturn("SELECT * FROM PAGE_VIEWS WHERE ID = 10;");
    when(statement.getSessionConfig()).thenReturn(sessionConfig);
    // We're slightly cheating here
    engineExecutor = spy(EngineExecutor.create(engineContext, serviceContext, sessionConfig));
    doReturn(immutableAnalysis).when(engineExecutor).getAnalysis(any());
    doReturn(logicalPlanNode).when(engineExecutor).buildAndValidateLogicalPlan(
        any(), any(), any(), any(), anyBoolean());
    doReturn(pullPhysicalPlan).when(engineExecutor).buildPullPhysicalPlan(any(), any());
  }


  @Test
  public void shouldRunPullQuery_success() {
    // Given:
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.complete(null);
    when(haRouting.handlePullQuery(eq(serviceContext), any(), eq(statement), eq(routingOptions),
        any(), any(), any())).thenReturn(future);

    // When:
    final PullQueryResult result = engineExecutor.executePullQuery(statement, haRouting,
        routingOptions, queryPlannerOptions, Optional.of(pullQueryMetrics), true);

    // Then:
    verify(haRouting).handlePullQuery(eq(serviceContext), any(), eq(statement), eq(routingOptions),
        any(), any(), any());
  }

  @Test
  public void shouldPassOnKsqlStatementException() {
    // Given:
    when(haRouting.handlePullQuery(eq(serviceContext), any(), eq(statement), eq(routingOptions),
        any(), any(), any())).thenThrow(
            new KsqlStatementException("Bad statement", "select * from foo;"));

    // When:
    final KsqlStatementException e = assertThrows(KsqlStatementException.class, () ->
        engineExecutor.executePullQuery(statement, haRouting,
            routingOptions, queryPlannerOptions, Optional.of(pullQueryMetrics), true));

    // Then:
    assertThat(e.getMessage(), is("Bad statement\nStatement: select * from foo;"));
  }

  @Test
  public void shouldPassOnOtherExceptions() {
    // Given:
    when(haRouting.handlePullQuery(eq(serviceContext), any(), eq(statement), eq(routingOptions),
        any(), any(), any())).thenThrow(new NullPointerException());

    // When:
    final RuntimeException e = assertThrows(RuntimeException.class, () ->
        engineExecutor.executePullQuery(statement, haRouting,
            routingOptions, queryPlannerOptions, Optional.of(pullQueryMetrics), true));

    // Then:
    assertThat(e.getMessage(), is("Error executing pull query"));
    assertThat((NullPointerException) e.getCause(), isA(NullPointerException.class));
  }
}
