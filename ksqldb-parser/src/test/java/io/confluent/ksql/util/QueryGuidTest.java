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

import java.time.LocalDateTime;
import org.junit.Assert;
import org.junit.Test;

public class QueryGuidTest {

  @Test
  public void shouldGetCorrectClusterNamespace() {
    // Given:
    final QueryGuid metaData = new QueryGuid("testData.testData",
        "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
            + "longitude DOUBLE)\nWITH (kafka_topic='locations', value_format='json', "
            + "partitions=1);", "TEST");

    // Then:
    Assert.assertEquals("testData.testData", metaData.getClusterNamespace());
  }

  @Test
  public void sameQueryInSameClusterAndOrgGetsSameId() {
    // Given:
    final String query1 = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, longitude "
        + "DOUBLE)\nWITH (kafka_topic='locations', value_format='json', partitions=1);";
    final String query2 = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
        + "longitude DOUBLE) WITH (kafka_topic='locations', value_format='json', partitions=1);";

    // When:
    final String queryId1 =
        new QueryGuid("testData.testData", query1, "TEST").getQueryGuid();
    final String queryId2 =
        new QueryGuid("testData.testData", query2, "TEST").getQueryGuid();

    // Then:
    Assert.assertEquals(queryId1, queryId2);
  }

  @Test
  public void sameQueryWithDifferentClusterOrOrgGetsDifferentId() {
    // Given:
    final String query = "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, longitude "
        + "DOUBLE)\nWITH (kafka_topic='locations', value_format='json', partitions=1);";

    // When:
    final String queryId1 =
        new QueryGuid("testData.testData", query, "TEST").getQueryGuid();
    final String queryId2 =
        new QueryGuid("testData2.testData", query, "TEST").getQueryGuid();

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
    final String id1 =
        new QueryGuid("testData.testData", query1, anonQuery).getStructuralGuid();
    final String id2 =
        new QueryGuid("testData.testData", query2, anonQuery).getStructuralGuid();

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
    final String id1 =
        new QueryGuid("testData.testData", "TEST", anonQuery1)
        .getStructuralGuid();
    final String id2 =
        new QueryGuid("testData.testData", "TEST", anonQuery2)
        .getStructuralGuid();

    // Then:
    Assert.assertNotEquals(id1, id2);
  }

  @Test
  public void queriesGetCorrectTimeOfCreation() {
    // Given:
    final LocalDateTime localDateTime = LocalDateTime.now();
    final QueryGuid metaData = new QueryGuid(
        "testData.testData",
        "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
            + "longitude DOUBLE)\nWITH (kafka_topic='locations', value_format='json', "
            + "partitions=1);",
        "TEST",
        localDateTime
    );

    // Then:
    Assert.assertEquals(localDateTime, metaData.getTimeOfCreation());
  }

  @Test
  public void handlesEmptyNamespaceCorrectly() {
    // Given:
    final LocalDateTime localDateTime = LocalDateTime.now();
    final QueryGuid metaData = new QueryGuid(
        "",
        "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
            + "longitude DOUBLE)\nWITH (kafka_topic='locations', value_format='json', "
            + "partitions=1);",
        "TEST",
        localDateTime
    );

    // Then:
    Assert.assertEquals("", metaData.getClusterNamespace());
    Assert.assertNotEquals("", metaData.getQueryGuid());
    Assert.assertNotEquals("", metaData.getStructuralGuid());
    Assert.assertEquals(localDateTime, metaData.getTimeOfCreation());
  }

  @Test
  public void handlesEmptyQueryCorrectly() {
    // Given:
    final LocalDateTime localDateTime = LocalDateTime.now();
    final QueryGuid metaData = new QueryGuid(
        "testData.testData",
        "",
        "TEST",
        localDateTime
    );

    // Then:
    Assert.assertEquals("testData.testData", metaData.getClusterNamespace());
    Assert.assertNotEquals("", metaData.getQueryGuid());
    Assert.assertNotEquals("", metaData.getStructuralGuid());
    Assert.assertEquals(localDateTime, metaData.getTimeOfCreation());
  }

  @Test
  public void handlesEmptyAnonQueryCorrectly() {
    // Given:
    final LocalDateTime localDateTime = LocalDateTime.now();
    final QueryGuid metaData = new QueryGuid(
        "testData.testData",
        "CREATE STREAM my_stream (profileId VARCHAR, latitude DOUBLE, "
            + "longitude DOUBLE)\nWITH (kafka_topic='locations', value_format='json', "
            + "partitions=1);",
        "TEST",
        localDateTime
    );

    // Then:
    Assert.assertEquals("testData.testData", metaData.getClusterNamespace());
    Assert.assertNotEquals("", metaData.getQueryGuid());
    Assert.assertNotEquals("", metaData.getStructuralGuid());
    Assert.assertEquals(localDateTime, metaData.getTimeOfCreation());
  }
}
