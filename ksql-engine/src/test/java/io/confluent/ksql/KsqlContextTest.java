/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetricsTestUtil;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Test;

public class KsqlContextTest {

  private static final String statement1 = "CREATE STREAM orders (ordertime bigint, orderid bigint, "
                                   + "itemid varchar, "
                      + "orderunits double, arraycol array<double>, mapcol map<varchar, double>) "
                      + "WITH (kafka_topic='ordertopic', value_format='JSON' , "
                      + "key='orderid');\n";
  private static final String statement2 = "CREATE STREAM BIGORDERS AS SELECT * FROM orders WHERE ORDERUNITS > 5;";

  @Test
  public void shouldRunSimpleStatements() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final KsqlEngine ksqlEngine = mock(KsqlEngine.class);
    expect(ksqlEngine.buildMultipleQueries(statement1, ksqlConfig, Collections.emptyMap()))
        .andReturn
        (Collections.emptyList());
    expect(ksqlEngine.buildMultipleQueries(statement2, ksqlConfig, Collections.emptyMap()))
        .andReturn(getQueryMetadata(new QueryId("CSAS_BIGORDERS"), DataSource.DataSourceType.KSTREAM));
    replay(ksqlEngine);

    final KsqlContext ksqlContext = new KsqlContext(ksqlConfig, ksqlEngine);
    ksqlContext.sql(statement1);
    ksqlContext.sql(statement2);

    verify(ksqlEngine);
  }

  @SuppressWarnings("unchecked")
  private List<QueryMetadata> getQueryMetadata(final QueryId queryid, final DataSource.DataSourceType type) {
    final KafkaStreams queryStreams = mock(KafkaStreams.class);
    queryStreams.start();
    expectLastCall();
    queryStreams.setStateListener(anyObject());
    expect(queryStreams.state()).andReturn(State.RUNNING);

    final OutputNode outputNode = mock(OutputNode.class);
    expect(outputNode.accept(anyObject(PlanSourceExtractorVisitor.class), anyObject())).andReturn(null);
    final StructuredDataSource structuredDataSource = mock(StructuredDataSource.class);
    expect(structuredDataSource.getName()).andReturn("");
    replay(structuredDataSource, outputNode, queryStreams);

    final PersistentQueryMetadata persistentQueryMetadata = new PersistentQueryMetadata(queryid.toString(),
                                                                                   queryStreams,
                                                                                   outputNode,
                                                                                  structuredDataSource,
                                                                                  "",
                                                                                  queryid,
                                                                                  type,
                                                                                  "KSQL_query_" + queryid,
                                                                                  null,
                                                                                  null,
                                                                                  null,
                                                                                  null);

    return Collections.singletonList(persistentQueryMetadata);
  }

}
