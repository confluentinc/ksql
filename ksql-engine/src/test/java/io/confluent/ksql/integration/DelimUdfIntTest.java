package io.confluent.ksql.integration;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.TestDataProvider;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class DelimUdfIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;
  private String topicName = "TestTopic";
  private TestDataProvider dataProvider;

  String format = DataSource.DataSourceSerDe.DELIMITED.name();

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness(format);
    testHarness.start();
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    testHarness.createTopic(topicName);

    /**
     * Setup test data
     */
    dataProvider = new ItemDataProvider();
    recordMetadataMap = testHarness.publishTestData(topicName, dataProvider, null);
    createStream(format);
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void testApplyUdfsToColumns() throws Exception {
    final String testStreamName = "SelectUDFsStream".toUpperCase();

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT %s FROM %s WHERE %s;",
            testStreamName,
            "ID, DESCRIPTION",
            "ITEMS",
            "ID LIKE '%_1'"
    );

    ksqlContext.sql(queryString);

    Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList( "ITEM_1", "home cinema")));

    Map<String, GenericRow> results = testHarness.consumeData(testStreamName, dataProvider.schema(), 1, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS);

    assertThat(results, equalTo(expectedResults));
  }

  private void createStream(String serdes) throws Exception {
    ksqlContext.sql("CREATE STREAM items (ID varchar, DESCRIPTION varchar) WITH (kafka_topic='TestTopic', value_format='" + serdes + "');");
  }
}
