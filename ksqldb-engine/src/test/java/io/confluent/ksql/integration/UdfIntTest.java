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

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.serde.FormatFactory.AVRO;
import static io.confluent.ksql.serde.FormatFactory.DELIMITED;
import static io.confluent.ksql.serde.FormatFactory.JSON;
import static io.confluent.ksql.serde.FormatFactory.KAFKA;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.serde.json.JsonFormat;
import io.confluent.ksql.test.util.KsqlIdentifierTestUtil;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.OrderDataProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.util.QueryMetadata;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
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

  private static Multimap<GenericKey, RecordMetadata> jsonRecordMetadataMap;
  private static Multimap<GenericKey, RecordMetadata> avroRecordMetadataMap;

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CLUSTER_WITH_RETRY = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://foo:8080")
      .build();

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

    jsonRecordMetadataMap = TEST_HARNESS.produceRows(JSON_TOPIC_NAME, orderDataProvider, KAFKA, JSON);

    avroRecordMetadataMap = TEST_HARNESS.produceRows(AVRO_TOPIC_NAME, orderDataProvider, KAFKA, AVRO);

    TEST_HARNESS.produceRows(DELIMITED_TOPIC_NAME, itemDataProvider, KAFKA, DELIMITED);
  }

  public UdfIntTest(final Format format) {
    switch (format.name()) {
      case AvroFormat.NAME:
        this.testData =
            new TestData(format, AVRO_TOPIC_NAME, AVRO_STREAM_NAME, avroRecordMetadataMap);
        break;
      case JsonFormat.NAME:
        this.testData =
            new TestData(format, JSON_TOPIC_NAME, JSON_STREAM_NAME, jsonRecordMetadataMap);
        break;
      default:
        this.testData =
            new TestData(format, DELIMITED_TOPIC_NAME, DELIMITED_STREAM_NAME,
                ImmutableListMultimap.of());
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
            + "ORDERID, "
            + "ITEMID, "
            + "ORDERUNITS*10, "
            + "PRICEARRAY[1]+10, "
            + "KEYVALUEMAP['key1'] * KEYVALUEMAP['key2']+10, "
            + "PRICEARRAY[2] > 1000 "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        resultStreamName,
        testData.sourceStreamName
    );

    // When:
    List<QueryMetadata> metadataList = ksqlContext.sql(queryString);

    // Then:
    final Map<GenericKey, GenericRow> results = consumeOutputMessages();

    assertThat(results, is(ImmutableMap.of(genericKey("ORDER_6"),
        genericRow("ITEM_8", 800.0, 1110.0, 12.0, true))));
    metadataList.forEach(q -> q.close());
  }

  @Test
  public void testShouldCastSelectedColumns() {
    Assume.assumeThat(testData.format, is(not(DELIMITED)));

    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "ORDERID, "
            + "CAST (ORDERUNITS AS INTEGER), "
            + "CAST( PRICEARRAY[2]>1000 AS STRING), "
            + "CAST (SUBSTRING(ITEMID, 6) AS DOUBLE), "
            + "CAST(ORDERUNITS AS VARCHAR) "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        resultStreamName,
        testData.sourceStreamName
    );

    // When:
    List<QueryMetadata> metadataList = ksqlContext.sql(queryString);

    // Then:
    final Map<GenericKey, GenericRow> results = consumeOutputMessages();

    assertThat(results, is(ImmutableMap.of(genericKey("ORDER_6"),
        genericRow(80, "true", 8.0, "80.0"))));
    metadataList.forEach(q -> q.close());
  }

  @Test
  public void testTimestampColumnSelection() {
    Assume.assumeThat(testData.format, is(not(DELIMITED)));

    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "ROWTIME+10000 AS RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';"
            + ""
            + "CREATE STREAM \"%s\" AS SELECT "
            + "ROWTIME AS NEWRTIME, RTIME, RT100, ORDERID, ITEMID "
            + "FROM %s;",
        intermediateStream, testData.sourceStreamName, resultStreamName, intermediateStream);

    // When:
    List<QueryMetadata> metadataList = ksqlContext.sql(queryString);

    // Then:
    final Map<GenericKey, GenericRow> results = consumeOutputMessages();

    final long ts = testData.getLast("ORDER_6").timestamp();

    assertThat(results, equalTo(ImmutableMap.of(genericKey("ORDER_6"),
        genericRow(ts, ts + 10000, ts + 100, "ITEM_8"))));
    metadataList.forEach(q -> q.close());
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
    List<QueryMetadata> metadataList = ksqlContext.sql(queryString);

    // Then:
    final Map<GenericKey, GenericRow> results = consumeOutputMessages();

    assertThat(results, equalTo(Collections.singletonMap(genericKey("ITEM_1"),
        genericRow("home cinema"))));
    metadataList.forEach(q -> q.close());
  }

  private void createSourceStream() {
    if (testData.format == DELIMITED) {
      // Delimited does not support array or map types, so use simplier schema:
      ksqlContext.sql(String.format("CREATE STREAM %s "
              + "(ID varchar KEY, DESCRIPTION varchar) WITH "
              + "(kafka_topic='%s', value_format='%s');",
          testData.sourceStreamName, testData.sourceTopicName, testData.format.name()));
    } else {
      ksqlContext.sql(String.format("CREATE STREAM %s ("
              + "ORDERID varchar KEY, "
              + "ORDERTIME bigint, "
              + "ITEMID varchar, "
              + "ORDERUNITS double, "
              + "TIMESTAMP varchar, "
              + "PRICEARRAY array<double>, "
              + "KEYVALUEMAP map<varchar, double>) "
              + "WITH (kafka_topic='%s', value_format='%s');",
          testData.sourceStreamName, testData.sourceTopicName, testData.format.name()));
    }
  }

  private Map<GenericKey, GenericRow> consumeOutputMessages() {

    final DataSource source = ksqlContext
        .getMetaStore()
        .getSource(SourceName.of(resultStreamName));

    final PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getKsqlTopic().getKeyFormat().getFeatures(),
        source.getKsqlTopic().getValueFormat().getFeatures()
    );

    return TEST_HARNESS.verifyAvailableUniqueRows(
        resultStreamName,
        1,
        KAFKA,
        testData.format,
        resultSchema);
  }

  private static class TestData {

    private final Format format;
    private final String sourceStreamName;
    private final String sourceTopicName;
    private final Multimap<GenericKey, RecordMetadata> recordMetadata;

    private TestData(
        final Format format,
        final String sourceTopicName,
        final String sourceStreamName,
        final Multimap<GenericKey, RecordMetadata> recordMetadata
    ) {
      this.format = format;
      this.sourceStreamName = sourceStreamName;
      this.sourceTopicName = sourceTopicName;
      this.recordMetadata = recordMetadata;
    }

    RecordMetadata getLast(final String key) {
      return Iterables.getLast(recordMetadata.get(genericKey(key)));
    }
  }
}
