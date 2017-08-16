package io.confluent.ksql.rest.client;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.mock.MockApplication;

public class KsqlRestClientTest {

  MockApplication mockApplication;
  int portNumber = 59098;
  KsqlRestConfig ksqlRestConfig;
  KsqlRestClient ksqlRestClient;

  @Before
  public void init() throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:59098");
//    props.put(KsqlRestConfig.PORT_CONFIG, String.valueOf(portNumber));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test");
    props.put(KsqlRestConfig.COMMAND_TOPIC_SUFFIX_CONFIG, "commands");
    ksqlRestConfig = new KsqlRestConfig(props);
    mockApplication = new MockApplication(ksqlRestConfig);
    mockApplication.start();

    ksqlRestClient = new KsqlRestClient("http://localhost:" + portNumber);
  }

  @After
  public void cleanUp() throws Exception {
    mockApplication.stop();
  }

  @Test
  public void testKsqlResource() {

    RestResponse<KsqlEntityList> results = ksqlRestClient.makeKsqlRequest("Test request");
    Assert.assertNotNull(results);
    Assert.assertTrue(results.isSuccessful());
    KsqlEntityList ksqlEntityList = results.getResponse();
    Assert.assertTrue(ksqlEntityList.size() == 1);
    Assert.assertTrue(ksqlEntityList.get(0) instanceof ExecutionPlan);
  }


  @Test
  public void testStreamQuery() {
    RestResponse<KsqlRestClient.QueryStream> queryResponse = ksqlRestClient.makeQueryRequest
        ("Select *");
    Assert.assertNotNull(queryResponse);
    Assert.assertTrue(queryResponse.isSuccessful());
  }

  @Test
  public void testStatus() {
    RestResponse<CommandStatuses> commandStatusesRestResponse = ksqlRestClient.makeStatusRequest();
    Assert.assertNotNull(commandStatusesRestResponse);
    Assert.assertTrue(commandStatusesRestResponse.isSuccessful());
    CommandStatuses commandStatuses = commandStatusesRestResponse.getResponse();
    Assert.assertTrue(commandStatuses.size() == 2);
    Assert.assertTrue(commandStatuses.get(new CommandId(CommandId.Type.TOPIC, "c1")) == CommandStatus.Status.SUCCESS);
    Assert.assertTrue(commandStatuses.get(new CommandId(CommandId.Type.TOPIC, "c2")) ==
                      CommandStatus.Status.ERROR);

  }

}
