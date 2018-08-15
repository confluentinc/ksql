package io.confluent.ksql.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.function.UdfCompiler;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceSerDe;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final IntegrationTestHarness TEST_HARNESS = new IntegrationTestHarness();

  private static Schema ITEM_DATA_SCHEMA;
  private static Map<String, RecordMetadata> jsonRecordMetadataMap;
  private static Map<String, RecordMetadata> avroRecordMetadataMap;

  private final DataSourceSerDe format;
  private final String sourceStreamName;
  private final Map<String, RecordMetadata> recordMetadata;

  private String resultStreamName;
  private KsqlContext ksqlContext;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<DataSource.DataSourceSerDe> formats() {
    return ImmutableList.of(DataSourceSerDe.AVRO, DataSourceSerDe.JSON);
  }

  @BeforeClass
  public static void classSetUp() throws Exception {
    TEST_HARNESS.start(Collections.emptyMap());

    TEST_HARNESS.createTopic(JSON_TOPIC_NAME);
    TEST_HARNESS.createTopic(AVRO_TOPIC_NAME);
    TEST_HARNESS.createTopic(DELIMITED_TOPIC_NAME);

    final OrderDataProvider orderDataProvider = new OrderDataProvider();
    final ItemDataProvider itemDataProvider = new ItemDataProvider();

    jsonRecordMetadataMap = ImmutableMap.copyOf(TEST_HARNESS.publishTestData(JSON_TOPIC_NAME,
        orderDataProvider,
        null,
        DataSource.DataSourceSerDe.JSON));

    avroRecordMetadataMap = ImmutableMap.copyOf(TEST_HARNESS.publishTestData(AVRO_TOPIC_NAME,
        orderDataProvider,
        null,
        DataSource.DataSourceSerDe.AVRO));

    TEST_HARNESS.publishTestData(DELIMITED_TOPIC_NAME,
        itemDataProvider,
        null,
        DataSource.DataSourceSerDe.DELIMITED);

    ITEM_DATA_SCHEMA = itemDataProvider.schema();
  }

  @AfterClass
  public static void classTearDown() {
    TEST_HARNESS.stop();
  }

  public UdfIntTest(final DataSource.DataSourceSerDe format) {
    this.format = format;
    switch (format) {
      case AVRO:
        this.sourceStreamName = AVRO_STREAM_NAME;
        this.recordMetadata = avroRecordMetadataMap;
        break;
      default:
        this.sourceStreamName = JSON_STREAM_NAME;
        this.recordMetadata = jsonRecordMetadataMap;
        break;
    }
  }

  @Before
  public void before() {
    resultStreamName = "OUTPUT-" + COUNTER.getAndIncrement();

    ksqlContext = KsqlContext.create(TEST_HARNESS.ksqlConfig, TEST_HARNESS.schemaRegistryClient);

    // load substring udf from classpath
    new UdfLoader(ksqlContext.getMetaStore(),
        TestUtils.tempDirectory(),
        UdfIntTest.class.getClassLoader(),
        value -> true, new UdfCompiler(Optional.empty()), Optional.empty(), true)
        .load();

    createSourceStreams();
  }

  @After
  public void after() {
    ksqlContext.close();
  }

  @Test
  public void testApplyUdfsToColumns() {
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
        sourceStreamName
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
    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "CAST (ORDERUNITS AS INTEGER), "
            + "CAST( PRICEARRAY[1]>1000 AS STRING), "
            + "CAST (SUBSTRING(ITEMID, 6) AS DOUBLE), "
            + "CAST(ORDERUNITS AS VARCHAR) "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID LIKE '%%_8';",
        resultStreamName,
        sourceStreamName
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
    // Given:
    final String originalStream = "ORIGINALSTREAM" + COUNTER.getAndIncrement();

    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT "
            + "ROWKEY AS RKEY, ROWTIME+10000 AS RTIME, ROWTIME+100 AS RT100, ORDERID, ITEMID "
            + "FROM %s WHERE ORDERUNITS > 20 AND ITEMID = 'ITEM_8';"
            + ""
            + "CREATE STREAM \"%s\" AS SELECT "
            + "ROWKEY AS NEWRKEY, ROWTIME AS NEWRTIME, RKEY, RTIME, RT100, ORDERID, ITEMID "
            + "FROM %s;",
        originalStream, sourceStreamName, resultStreamName, originalStream);

    // When:
    ksqlContext.sql(queryString);

    // Then:
    final Map<String, GenericRow> results = consumeOutputMessages();

    final long ts = recordMetadata.get("8").timestamp();

    assertThat(results, equalTo(ImmutableMap.of("8",
        new GenericRow(Arrays.asList("8", ts, "8", ts + 10000, ts + 100, "ORDER_6", "ITEM_8")))));
  }

  @Test
  public void testApplyUdfsToColumnsDelimited() {
    // Given:
    final String queryString = String.format(
        "CREATE STREAM \"%s\" AS SELECT ID, DESCRIPTION FROM %s WHERE ID LIKE '%%_1';",
        resultStreamName, DELIMITED_STREAM_NAME
    );

    // When:
    ksqlContext.sql(queryString);

    // Then:
    final Map<String, GenericRow> results =
        consumeOutputMessages(ITEM_DATA_SCHEMA, DataSource.DataSourceSerDe.DELIMITED);

    assertThat(results, equalTo(Collections.singletonMap("ITEM_1",
        new GenericRow(Arrays.asList("ITEM_1", "home cinema")))));
  }

  private void createSourceStreams() {
    createOrdersStream(JSON_TOPIC_NAME, JSON_STREAM_NAME, DataSourceSerDe.JSON);
    createOrdersStream(AVRO_TOPIC_NAME, AVRO_STREAM_NAME, DataSourceSerDe.AVRO);

    ksqlContext.sql(String.format("CREATE STREAM %s (ID varchar, DESCRIPTION varchar) WITH "
            + "(kafka_topic='%s', value_format='%s');",
        DELIMITED_STREAM_NAME,
        DELIMITED_TOPIC_NAME,
        DataSource.DataSourceSerDe.DELIMITED.name()));
  }

  private void createOrdersStream(
      final String topicName, final String streamName, final DataSource.DataSourceSerDe format) {

    ksqlContext.sql(String.format("CREATE STREAM %s ("
            + "ORDERTIME bigint, "
            + "ORDERID varchar, "
            + "ITEMID varchar, "
            + "ORDERUNITS double, "
            + "PRICEARRAY array<double>, "
            + "KEYVALUEMAP map<varchar, double>) "
            + "WITH (kafka_topic='%s', value_format='%s');",
        streamName,
        topicName,
        format.name()));
  }

  private Map<String, GenericRow> consumeOutputMessages() {
    final Schema resultSchema = SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
        ksqlContext.getMetaStore().getSource(resultStreamName).getSchema());

    return consumeOutputMessages(resultSchema, format);
  }

  private Map<String, GenericRow> consumeOutputMessages(
      final Schema resultSchema,
      final DataSourceSerDe serDeFormat) {

    return TEST_HARNESS.consumeData(
        resultStreamName,
        resultSchema,
        1,
        new StringDeserializer(),
        IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS,
        serDeFormat);
  }
}
