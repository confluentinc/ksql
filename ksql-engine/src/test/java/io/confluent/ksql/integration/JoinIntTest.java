package io.confluent.ksql.integration;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.ItemDataProvider;
import io.confluent.ksql.util.OrderDataProvider;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.ThreadFactory;

import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class JoinIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;


  private String orderStreamTopic = "OrderTopic";
  private OrderDataProvider orderDataProvider;
  private Map<String, RecordMetadata> orderRecordMetadataMap;

  private String itemTableTopic = "ItemTopic";
  private ItemDataProvider itemDataProvider;
  private Map<String, RecordMetadata> itemRecordMetadataMap;
  private final long now = System.currentTimeMillis();

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness(DataSource.DataSourceSerDe.JSON.name());
    testHarness.start();
    Map<String, Object> ksqlStreamConfigProps = testHarness.ksqlConfig.getKsqlStreamConfigProps();
    // turn caching off to improve join consistency
    ksqlStreamConfigProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    ksqlContext = KsqlContext.create(ksqlStreamConfigProps);

    /**
     * Setup test data
     */
    testHarness.createTopic(itemTableTopic);
    itemDataProvider = new ItemDataProvider();

    itemRecordMetadataMap = testHarness.publishTestData(itemTableTopic, itemDataProvider, now -500);


    testHarness.createTopic(orderStreamTopic);
    orderDataProvider = new OrderDataProvider();
    orderRecordMetadataMap = testHarness.publishTestData(orderStreamTopic, orderDataProvider, now);
    createStreams();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }


  int failRate = 0;
  @Test
  public void collectSuccessRate() throws Exception {

    int firstTimeSuccessRate = 0;
    int totalFails = 0;
    for (int i = 1; i < 10; i++) {
      failRate = 0;
      before();
      shouldLeftJoinOrderAndItems();
      after();
      if (failRate >0) totalFails++;
      else firstTimeSuccessRate++;
      System.out.println(new Date() + "Iteration:" + i + " firstTimeSucess:" + firstTimeSuccessRate + " failsThisIter:" + failRate + " totalFails:" + totalFails + " Failure:" + ( (double)totalFails / (double) i) * 100 + "%\n\n\n\n");
    }
    System.out.println("FAILURE_RATE:" + failRate);
  }

  @Test
  public void shouldLeftJoinOrderAndItems() throws Exception {
    final String testStreamName = "OrderedWithDescription".toUpperCase();

    final String queryString = String.format(
            "CREATE STREAM %s AS SELECT ORDERID, ITEMID, ORDERUNITS, DESCRIPTION FROM orders LEFT JOIN items ON orders.ITEMID = items.ID WHERE orders.ITEMID = 'ITEM_1' ;",
            testStreamName
    );

    ksqlContext.sql(queryString);

    Schema resultSchema = ksqlContext.getMetaStore().getSource(testStreamName).getSchema();

    Map<String, GenericRow> expectedResults = Collections.singletonMap("ITEM_1", new GenericRow(Arrays.asList(null, null, "ORDER_1", "ITEM_1", 10.0, "home cinema")));

    final Map<String, GenericRow> results = new HashMap<>();
    TestUtils.waitForCondition(() -> {
      results.putAll(testHarness.consumeData(testStreamName, resultSchema, 1, new StringDeserializer(), IntegrationTestHarness.RESULTS_POLL_MAX_TIME_MS));
      final boolean success = results.equals(expectedResults);
      System.out.println("Results:" + results.toString().replace("],", "],\n"));

      if (!success) {

        System.out.println("FAIL");
        System.exit(1);
        try {
          failRate++;
          // The join may not be triggered fist time around due to order in which the
          // consumer pulls the records back. So we publish again to make the stream
          // trigger the join.
          testHarness.publishTestData(orderStreamTopic, orderDataProvider, now);
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        System.out.println("SUCCESS");
      }
      return success;
    }, 60000, "failed to complete join correctly");
  }

  private void createStreams() throws Exception {
    ksqlContext.sql("CREATE STREAM orders (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='" + orderStreamTopic + "', value_format='JSON');");
    ksqlContext.sql("CREATE TABLE items (ID varchar, DESCRIPTION varchar) WITH (key='ID', kafka_topic='" + itemTableTopic + "', value_format='JSON');");
  }

}
