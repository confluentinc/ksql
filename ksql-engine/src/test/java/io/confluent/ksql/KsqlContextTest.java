/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import java.util.Collections;
import org.junit.Before;
import java.util.List;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlContextTest {

  private static final KsqlConfig SOME_CONFIG = new KsqlConfig(Collections.emptyMap());

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KsqlEngine ksqlEngine;
  @Mock
  private PersistentQueryMetadata persistentQuery;
  @Mock
  private QueuedQueryMetadata transientQuery;
  private KsqlContext ksqlContext;

  @Before
  public void setUp() {
    ksqlContext = new KsqlContext(serviceContext, SOME_CONFIG, ksqlEngine);
  }

  @Test
  public void shouldInvokeEngineWithCorrectParams() {
    // When:
    ksqlContext.sql("Some SQL", ImmutableMap.of("overridden", "props"));

    // Then:
    verify(ksqlEngine).execute("Some SQL", SOME_CONFIG, ImmutableMap.of("overridden", "props"));
  }

  @Test
  public void shouldStartPersistentQueries() {
    // Given:
    when(ksqlEngine.execute(any(), any(), any()))
        .thenReturn(ImmutableList.of(persistentQuery));

    // When:
    ksqlContext.sql("Some SQL", ImmutableMap.of("overridden", "props"));

    // Then:
    verify(persistentQuery).start();
  }

  @Test
  public void shouldNotBlowUpOnSqlThatDoesNotResultInPersistentQueries() {
    // Given:
    when(ksqlEngine.execute(any(), any(), any()))
        .thenReturn(ImmutableList.of(transientQuery));

    // When:
    ksqlContext.sql("Some SQL", ImmutableMap.of("overridden", "props"));

    // Then:
    // Did not blow up.
  }

  @Test
  public void shouldCloseEngineBeforeServiceContextOnClose() {
    // When:
    ksqlContext.close();

    // Then:
    final InOrder inOrder = inOrder(ksqlEngine, serviceContext);
    inOrder.verify(ksqlEngine).close();
    inOrder.verify(serviceContext).close();
  }
}
