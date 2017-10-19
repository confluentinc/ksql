package io.confluent.ksql.integration;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Set;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlContext;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.OrderDataProvider;
import io.confluent.ksql.util.QueryMetadata;

import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class ResourceCleanUpIntTest {

  private IntegrationTestHarness testHarness;
  private KsqlContext ksqlContext;
  private Map<String, RecordMetadata> recordMetadataMap;
  private final String topicName = "TestTopic";
  private OrderDataProvider dataProvider;

  final String streamName = "TUMBLING_AGGTEST";

  final String queryString = String.format(
      "CREATE TABLE %s AS SELECT %s FROM ORDERS WINDOW %s WHERE ITEMID = 'ITEM_1' GROUP BY ITEMID;",
      streamName,
      "ITEMID, COUNT(ITEMID), SUM(ORDERUNITS)",
      "TUMBLING ( SIZE 10 SECONDS)"
  );

  @Before
  public void before() throws Exception {
    testHarness = new IntegrationTestHarness();
    testHarness.start();
    ksqlContext = KsqlContext.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    testHarness.createTopic(topicName);

    /**
     * Setup test data
     */
    dataProvider = new OrderDataProvider();
    recordMetadataMap = testHarness.publishTestData(topicName, dataProvider, null );
    createOrdersStream();
  }

  @After
  public void after() throws Exception {
    ksqlContext.close();
    testHarness.stop();
  }

  @Test
  public void testInternalTopicCleanup() throws Exception {

    ksqlContext.sql(queryString);
    Schema resultSchema = ksqlContext.getMetaStore().getSource(streamName).getSchema();

    Map<Windowed<String>, GenericRow> results = testHarness.consumeData(streamName, resultSchema,
                                                                        1, new WindowedDeserializer<>(new StringDeserializer()));

    AdminClient adminClient = AdminClient.create(testHarness.ksqlConfig.getKsqlStreamConfigProps());
    KafkaTopicClient topicClient = new KafkaTopicClientImpl(adminClient);

    Set<String> topicBeforeCleanup = topicClient.listTopicNames();

    assertThat("Expected to have 4 topics instead have : " + topicBeforeCleanup.size(),
               topicBeforeCleanup.size() == 4);
    QueryMetadata queryMetadata = ksqlContext.getRunningQueries().iterator().next();

    queryMetadata.close();
    Set<String> topicsAfterCleanUp = topicClient.listTopicNames();

    assertThat("Expected to see two topics after clean up but seeing " + topicsAfterCleanUp.size
        (), topicsAfterCleanUp.size() == 2);
  }


  private void createOrdersStream() throws Exception {
    ksqlContext.sql("CREATE STREAM ORDERS (ORDERTIME bigint, ORDERID varchar, ITEMID varchar, ORDERUNITS double, PRICEARRAY array<double>, KEYVALUEMAP map<varchar, double>) WITH (kafka_topic='TestTopic', value_format='JSON', key='ordertime');");
  }

}
