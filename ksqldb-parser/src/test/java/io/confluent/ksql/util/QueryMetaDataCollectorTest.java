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

package io.confluent.ksql.util;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryMetaDataCollectorTest {
  private KsqlConfig ksqlConfig;

  @Before
  public void setUp() {
    // initialize ksql config
    Map<String, String> ksqlConfigMap = new HashMap<>();
    ksqlConfigMap.put(KsqlConfig.KSQL_CCLOUD_QUERYANONYMIZER_CLUSTER_NAMESPACE,
        "testData.testData");
    ksqlConfig = new KsqlConfig(ksqlConfigMap);
  }

  @Test
  public void shouldGetCorrectClusterNamespaceData() {
    // Given:
    final Map<String, String> metaData = QueryMetaDataCollector.buildMetaData(ksqlConfig,
        "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
            + "longitude DOUBLE)\nWITH (kafka_topic='locations', value_format='json', "
            + "partitions=1);", "TEST");

    // Then:
    Assert.assertEquals("testData.testData",
        metaData.get("cluster_namespace"));
  }

  @Test
  public void sameQueryInSameClusterAndOrgGetsSameId() {
    // Given:
    final String query1 = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, longitude "
        + "DOUBLE)\nWITH (kafka_topic='locations', value_format='json', partitions=1);";
    final String query2 = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
        + "longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);";

    // When:
    final String queryId1 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, query1, "TEST").get("id");
    final String queryId2 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, query2, "TEST").get("id");

    // Then:
    Assert.assertEquals(queryId1, queryId2);
  }

  @Test
  public void sameQueryWithDifferentClusterOrOrgGetsDifferentId() {
    // Given:
    final String query = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, longitude "
        + "DOUBLE)\nWITH (kafka_topic='locations', value_format='json', partitions=1);";
    Map<String, String> ksqlConfigMap2 = new HashMap<>();
    ksqlConfigMap2.put(KsqlConfig.KSQL_CCLOUD_QUERYANONYMIZER_CLUSTER_NAMESPACE, "pc_id:testData2,org_id:testData");
    final KsqlConfig ksqlConfig2 = new KsqlConfig(ksqlConfigMap2);


    // When:
    final String queryId1 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, query, "TEST").get("id");
    final String queryId2 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig2, query, "TEST").get("id");


    // Then:
    Assert.assertNotEquals(queryId1, queryId2);
  }

  @Test
  public void queriesWithSameAnonFormShouldGetSameStructurallySimilarId() {
    // Given:
    final String query1 = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE) "
        + "WITH (kafka_topic='locations', value_format='json', partitions=1);";
    final String query2 = "CREATE STREAM my_stream (userId VARCHAR, performance DOUBLE) "
        + "WITH (kafka_topic='user_performance', value_format='json', partitions=2);";
    final String anonQuery = "CREATE STREAM stream1 (column1 VARCHAR, column2 DOUBLE) WITH "
        + "(kafka_topic=['string'], value_format=['string'], partitions='0';";

    // When:
    final String id1 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, query1, anonQuery)
        .get("structurally_similar_id");
    final String id2 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, query2, anonQuery)
        .get("structurally_similar_id");

    // Then:
    Assert.assertEquals(id1, id2);
  }

  @Test
  public void queriesWithDifferentAnonFormShouldGetSameStructurallySimilarId() {
    // Given:
    final String anonQuery1 = "CREATE STREAM stream1 (column1 VARCHAR, column2 DOUBLE) WITH "
        + "(kafka_topic=['string'], value_format=['string'], partitions='0';";
    final String anonQuery2 = "CREATE STREAM stream1 (column1 VARCHAR, column2 INT) WITH "
        + "(kafka_topic=['string'], value_format=['string'], partitions='0';";

    // When:
    final String id1 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, "TEST", anonQuery1)
        .get("structurally_similar_id");
    final String id2 = QueryMetaDataCollector
        .buildMetaData(ksqlConfig, "TEST", anonQuery2)
        .get("structurally_similar_id");

    // Then:
    Assert.assertNotEquals(id1, id2);
  }
}
