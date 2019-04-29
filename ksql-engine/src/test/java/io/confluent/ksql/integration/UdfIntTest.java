/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.integration;

import static io.confluent.ksql.serde.Format.AVRO;
import static io.confluent.ksql.serde.Format.DELIMITED;
import static io.confluent.ksql.serde.Format.JSON;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class UdfIntTest {

  private static final String JSON_TOPIC_NAME = "jsonTopic";
  private static final String JSON_STREAM_NAME = "orders_json";
  private static final String AVRO_TOPIC_NAME = "avroTopic";
  private static final String AVRO_STREAM_NAME = "orders_avro";
  private static final String DELIMITED_TOPIC_NAME = "delimitedTopic";
  private static final String DELIMITED_STREAM_NAME = "items_delimited";

  private static Map<String, RecordMetadata> jsonRecordMetadataMap;
  private static Map<String, RecordMetadata> avroRecordMetadataMap;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.buildKsqlContext();

  private final TestData testData;

  private String resultStreamName;
  private String intermediateStream;


  @Parameterized.Parameters(name = "{0}")
  public static Collection<Format> formats() {
    return ImmutableList.of(AVRO, JSON, DELIMITED);
  }

  @BeforeClass
  public static void classSetUp() {
    TEST_HARNESS.ensureTopics(JSON_TOPIC_NAME, AVRO_TOPIC_NAME, DELIMITED_TOPIC_NAME);

    final OrderDataProvider orderDataProvider = new OrderDataProvider();
    final ItemDataProvider itemDataProvider = new ItemDataProvider();

    jsonRecordMetadataMap = ImmutableMap.copyOf(
        TEST_HARNESS.produceRows(JSON_TOPIC_NAME, orderDataProvider, JSON));

    avroRecordMetadataMap = ImmutableMap.copyOf(
        TEST_HARNESS.produceRows(AVRO_TOPIC_NAME, orderDataProvider, AVRO));

    TEST_HARNESS.produceRows(DELIMITED_TOPIC_NAME, itemDataProvider, DELIMITED);
  }

  public UdfIntTest(final Format format) {
    switch (format) {
      case AVRO:
        this.testData =
            new TestData(format, AVRO_TOPIC_NAME, AVRO_STREAM_NAME, avroRecordMetadataMap);
        break;
      case JSON:
        this.testData =
            new TestData(format, JSON_TOPIC_NAME, JSON_STREAM_NAME, jsonRecordMetadataMap);
        break;
      default:
        this.testData =
            new TestData(format, DELIMITED_TOPIC_NAME, DELIMITED_STREAM_NAME, ImmutableMap.of());
        break;
    }
  }

  @Before
  public void before() {
    intermediateStream = KsqlIdentifierTestUtil.uniqueIdentifierName("INTERMEDIATE");
    resultStreamName = KsqlIdentifierTestUtil.uniqueIdentifierName("OUTPUT");

    createSourceStream();
  }

  @Test
  public void testApplyUdfsToColumns() {
    Assume.assumeThat(testData.format, is(not(DELIMITED)));

    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "ITEMID, "
            + "ORDERUNITS*10, "
            + "PRICEARRAY[0]+10, "
            + "KEYVALUEMAP['key1'] * KEYVALUEMAP['key2']+10, "
            + "PRICEARRAY[1] > 1000 "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        resultStreamName,
        testData.sourceStreamName
    );

    // When:
    ksqlContext.sql(queryString);

    // Then:
    final Map<String, GenericRow> results = consumeOutputMessages();

    assertThat(results, is(ImmutableMap.of("8",
        new GenericRow(Arrays.asList("ITEM_8", 800.0, 1110.0, 12.0, true)))));
  }

  @Test
  public void testShouldCastSelectedColumns() {
    Assume.assumeThat(testData.format, is(not(DELIMITED)));

    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "CAST (ORDERUNITS AS INTEGER), "
            + "CAST( PRICEARRAY[1]>1000 AS STRING), "
            + "CAST (SUBSTRING(ITEMID, 6) AS DOUBLE), "
            + "CAST(ORDERUNITS AS VARCHAR) "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        resultStreamName,
        testData.sourceStreamName
    );

    // When:
    ksqlContext.sql(queryString);

    // Then:
    final Map<String, GenericRow> results = consumeOutputMessages();

    assertThat(results, is(ImmutableMap.of("8",
        new GenericRow(Arrays.asList(80, "true", 8.0, "80.0")))));
  }

  @Test
  public void testTimestampColumnSelection() {
    Assume.assumeThat(testData.format, is(not(DELIMITED)));

    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "ROWKEY AS RKEY, ROWTIME+10000 AS RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';"
            + ""
            + "CREATE STREAM \"%s\" AS SELECT "
            + "ROWKEY AS NEWRKEY, ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
            + "FROM %s;",
        intermediateStream, testData.sourceStreamName, resultStreamName, intermediateStream);

    // When:
    ksqlContext.sql(queryString);

    // Then:
    final Map<String, GenericRow> results = consumeOutputMessages();

    final long ts = testData.recordMetadata.get("8").timestamp();

    assertThat(results, equalTo(ImmutableMap.of("8",
        new GenericRow(Arrays.asList("8", ts, "8", ts + 10000, ts + 100, "ORDER_6", "ITEM_8")))));
  }

  @Test
  public void testApplyUdfsToColumnsDelimited() {
    Assume.assumeThat(testData.format, is(DELIMITED));

    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT ID, DESCRIPTION FROM %s WHERE ID LIKE '%%_1';",
        resultStreamName, DELIMITED_STREAM_NAME
    );

    // When:
    ksqlContext.sql(queryString);

    // Then:
    final Map<String, GenericRow> results = consumeOutputMessages();

    assertThat(results, equalTo(Collections.singletonMap("ITEM_1",
        new GenericRow(Arrays.asList("ITEM_1", "home cinema")))));
  }

  private void createSourceStream() {
    if (testData.format == DELIMITED) {
      // Delimited does not support array or map types, so use simplier schema:
      ksqlContext.sql(String.format("CREATE STREAM %s (ID varchar, DESCRIPTION varchar) WITH "
              + "(kafka_topic='%s', value_format='%s');",
          testData.sourceStreamName, testData.sourceTopicName, testData.format.name()));
    } else {
      ksqlContext.sql(String.format("CREATE STREAM %s ("
              + "ORDERTIME bigint, "
              + "ORDERID varchar, "
              + "ITEMID varchar, "
              + "ORDERUNITS double, "
              + "TIMESTAMP varchar, "
              + "PRICEARRAY array<double>, "
              + "KEYVALUEMAP map<varchar, double>) "
              + "WITH (kafka_topic='%s', value_format='%s');",
          testData.sourceStreamName, testData.sourceTopicName, testData.format.name()));
    }
  }

  private Map<String, GenericRow> consumeOutputMessages() {
    final Schema resultSchema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
        ksqlContext.getMetaStore().getSource(resultStreamName).getSchema());

    return TEST_HARNESS.verifyAvailableUniqueRows(
        resultStreamName,
        1,
        testData.format,
        resultSchema);
  }

  private static class TestData {

    private final Format format;
    private final String sourceStreamName;
    private final String sourceTopicName;
    private final Map<String, RecordMetadata> recordMetadata;

    private TestData(
        final Format format,
        final String sourceTopicName,
        final String sourceStreamName,
        final Map<String, RecordMetadata> recordMetadata) {
      this.format = format;
      this.sourceStreamName = sourceStreamName;
      this.sourceTopicName = sourceTopicName;
      this.recordMetadata = recordMetadata;
    }
  }
}
