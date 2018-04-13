/**
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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import static org.easymock.EasyMock.*;

public class KsqlContextTest {

  private final String statement1 = "CREATE STREAM orders (ordertime bigint, orderid bigint, "
                                   + "itemid varchar, "
                      + "orderunits double, arraycol array<double>, mapcol map<varchar, double>) "
                      + "WITH (kafka_topic='ordertopic', value_format='JSON' , "
                      + "key='orderid');\n";
  private final String statement2 = "CREATE STREAM BIGORDERS AS SELECT * FROM orders WHERE ORDERUNITS > 5;";

  @Test
  public void shouldRunSimpleStatements() throws Exception {
    AdminClient adminClient = mock(AdminClient.class);
    KafkaTopicClient kafkaTopicClient = mock(KafkaTopicClientImpl.class);
    KsqlEngine ksqlEngine = mock(KsqlEngine.class);

    Map<QueryId, PersistentQueryMetadata> liveQueryMap = new HashMap<>();

    KsqlContext ksqlContext = new KsqlContext(adminClient, kafkaTopicClient, ksqlEngine);

    expect(ksqlEngine.buildMultipleQueries(statement1, Collections.emptyMap()))
        .andReturn
        (Collections.emptyList());
    expect(ksqlEngine.buildMultipleQueries(statement2, Collections.emptyMap()))
        .andReturn(getQueryMetadata(new QueryId("CSAS_BIGORDERS"), DataSource.DataSourceType.KSTREAM));
    expect(ksqlEngine.getPersistentQueries()).andReturn(liveQueryMap);
    replay(ksqlEngine);
    ksqlContext.sql(statement1);
    ksqlContext.sql(statement2);

    verify(ksqlEngine);
  }

  @SuppressWarnings("unchecked")
  private List<QueryMetadata> getQueryMetadata(QueryId queryid, DataSource.DataSourceType type) {
    KafkaStreams queryStreams = mock(KafkaStreams.class);
    queryStreams.start();
    expectLastCall();

    OutputNode outputNode = mock(OutputNode.class);
    expect(outputNode.accept(anyObject(PlanSourceExtractorVisitor.class), anyObject())).andReturn(null);
    replay(outputNode);
    StructuredDataSource structuredDataSource = mock(StructuredDataSource.class);
    expect(structuredDataSource.getName()).andReturn("");
    replay(structuredDataSource);

    PersistentQueryMetadata persistentQueryMetadata = new PersistentQueryMetadata(queryid.toString(),
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
