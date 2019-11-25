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

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.TerminateAllQueries;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminateAllQueriesExecutorTest {

  private static final KsqlConfig SOME_CONFIG = new KsqlConfig(ImmutableMap.of());
  private static final Map<String, Object> SOME_OVERRIDES = ImmutableMap.of("thing", "v");
  private static final QueryId ID_0 = new QueryId("0");
  private static final QueryId ID_1 = new QueryId("1");

  @Mock
  private DistributingExecutor distributor;
  @Mock
  private PersistentQueryMetadata query0;
  @Mock
  private PersistentQueryMetadata query1;
  @Mock
  private KsqlEntity entity0;
  @Mock
  private KsqlEntity entity1;
  @Mock
  private KsqlEntity entity2;
  @Mock
  private KsqlEngine engine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private ConfiguredStatement<TerminateAllQueries> terminateAll;
  private TerminateAllQueriesExecutor executor;

  @Before
  public void setUp() {
    executor = new TerminateAllQueriesExecutor(distributor);

    when(terminateAll.getConfig()).thenReturn(SOME_CONFIG);
    when(terminateAll.getOverrides()).thenReturn(SOME_OVERRIDES);

    when(query0.getQueryId()).thenReturn(ID_0);
    when(query1.getQueryId()).thenReturn(ID_1);
  }

  @Test
  public void shouldDistributeTerminatesForEachQuery() {
    // Given:
    when(engine.getPersistentQueries())
        .thenReturn(ImmutableList.of(query0, query1));

    // When:
    executor.execute(terminateAll, ImmutableMap.of(), engine, serviceContext);

    // Then:
    final InOrder inOrder = inOrder(distributor);
    inOrder.verify(distributor).execute(
        ConfiguredStatement.of(
            PreparedStatement.of(
                "TERMINATE " + ID_0 + ";",
                new TerminateQuery(Optional.empty(), ID_0)
            ),
            SOME_OVERRIDES,
            SOME_CONFIG
        ),
        ImmutableMap.of(),
        engine,
        serviceContext
    );
    inOrder.verify(distributor).execute(
        ConfiguredStatement.of(
            PreparedStatement.of(
                "TERMINATE " + ID_1 + ";",
                new TerminateQuery(Optional.empty(), ID_1)
            ),
            SOME_OVERRIDES,
            SOME_CONFIG
        ),
        ImmutableMap.of(),
        engine,
        serviceContext
    );
  }

  @Test
  public void shouldReturnEntitiesFromDistributed() {
    // Given:
    when(engine.getPersistentQueries())
        .thenReturn(ImmutableList.of(query0, query1));

    doReturn(
        ImmutableList.of(entity0),
        ImmutableList.of(entity1, entity2)
    )
        .when(distributor)
        .execute(any(), any(), any(), any());

    // When:
    final List<? extends KsqlEntity> result = executor
        .execute(terminateAll, ImmutableMap.of(), engine, serviceContext);

    // Then:
    assertThat(result, is(ImmutableList.of(entity0, entity1, entity2)));
  }
}