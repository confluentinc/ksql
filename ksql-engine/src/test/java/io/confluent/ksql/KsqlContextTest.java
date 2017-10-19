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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private final String statement3 = "CREATE TABLE ORDERSUMS AS select itemid, sum(orderunits) from orders window "
                      + "TUMBLING ( size 30 second) group by itemid;";

  @Test
  public void shouldRunSimpleStatements() throws Exception {
    AdminClient adminClient = mock(AdminClient.class);
    KafkaTopicClient kafkaTopicClient = mock(KafkaTopicClientImpl.class);
    KsqlEngine ksqlEngine = mock(KsqlEngine.class);

    Map<Long, PersistentQueryMetadata> liveQueryMap = new HashMap<>();

    KsqlContext ksqlContext = new KsqlContext(adminClient, kafkaTopicClient, ksqlEngine);

    expect(ksqlEngine.buildMultipleQueries(false, statement1, Collections.emptyMap()))
        .andReturn
        (Collections.emptyList());
    expect(ksqlEngine.buildMultipleQueries(false, statement2, Collections.emptyMap()))
        .andReturn(getQueryMetadata(1, DataSource.DataSourceType.KSTREAM));
    expect(ksqlEngine.buildMultipleQueries(false, statement3, Collections.emptyMap()))
        .andReturn(getQueryMetadata(2, DataSource.DataSourceType.KTABLE));
    expect(ksqlEngine.getPersistentQueries()).andReturn(liveQueryMap).times(2);
    replay(ksqlEngine);
    ksqlContext.sql(statement1);
    ksqlContext.sql(statement2);
    ksqlContext.sql(statement3);

    verify(ksqlEngine);
  }

  private List<QueryMetadata> getQueryMetadata(long queryid, DataSource.DataSourceType type) {
    KafkaStreams queryStreams = mock(KafkaStreams.class);
    queryStreams.start();
    expectLastCall();
    PersistentQueryMetadata persistentQueryMetadata1 = new PersistentQueryMetadata(String.valueOf
        (queryid),
                                                                                   queryStreams,
                                                                                  null,
                                                                                  "",
                                                                                  queryid,
                                                                                  type,
                                                                                  "KSQL_query_" + queryid,
                                                                                  null,
                                                                                  null);

    return Arrays.asList(persistentQueryMetadata1);

  }

}
