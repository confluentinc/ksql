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

package io.confluent.ksql.rest.client;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.mock.MockStreamedQueryResource;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.mock.MockApplication;
import io.confluent.ksql.rest.server.utils.TestUtils;

public class KsqlRestClientTest {

  MockApplication mockApplication;
  KsqlRestConfig ksqlRestConfig;
  KsqlRestClient ksqlRestClient;

  @Before
  public void init() throws Exception {
    final int port = TestUtils.randomFreeLocalPort();
    Map<String, Object> props = new HashMap<>();
    props.put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:" + port);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test");
    ksqlRestConfig = new KsqlRestConfig(props);
    mockApplication = new MockApplication(ksqlRestConfig);
    mockApplication.start();

    ksqlRestClient = new KsqlRestClient("http://localhost:" + port);
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
  public void testStreamRowFromServer() throws InterruptedException {
    MockStreamedQueryResource sqr = mockApplication.getStreamedQueryResource();
    RestResponse<KsqlRestClient.QueryStream> queryResponse = ksqlRestClient.makeQueryRequest
            ("Select *");
    Assert.assertNotNull(queryResponse);
    Assert.assertTrue(queryResponse.isSuccessful());

    // Get the stream writer from the mock server and load it up with a row
    List<MockStreamedQueryResource.TestStreamWriter> writers = sqr.getWriters();
    Assert.assertEquals(1, writers.size());
    MockStreamedQueryResource.TestStreamWriter writer = writers.get(0);
    try {
      writer.enq("hello");

      // Try and receive the row. Do this from another thread to avoid blocking indefinitely
      KsqlRestClient.QueryStream queryStream = queryResponse.getResponse();
      Thread t = new Thread(() -> queryStream.hasNext());
      t.setDaemon(true);
      t.start();
      t.join(10000);
      Assert.assertFalse(t.isAlive());
      Assert.assertTrue(queryStream.hasNext());

      StreamedRow sr = queryStream.next();
      Assert.assertNotNull(sr);
      GenericRow row = sr.getRow();
      Assert.assertEquals(1, row.getColumns().
              size());
      Assert.assertEquals("hello", row.getColumns().
              get(0));
    } finally {
      writer.finished();
    }
  }

  @Test
  public void shouldInterruptScannerOnClose() throws InterruptedException {
    MockStreamedQueryResource sqr = mockApplication.getStreamedQueryResource();
    RestResponse<KsqlRestClient.QueryStream> queryResponse = ksqlRestClient.makeQueryRequest
            ("Select *");
    Assert.assertNotNull(queryResponse);
    Assert.assertTrue(queryResponse.isSuccessful());

    List<MockStreamedQueryResource.TestStreamWriter> writers = sqr.getWriters();
    Assert.assertEquals(1, writers.size());
    try {
      // Try and receive a row. This will block since there is no data to return
      KsqlRestClient.QueryStream queryStream = queryResponse.getResponse();
      CountDownLatch threw = new CountDownLatch(1);
      Thread t = new Thread(() -> {
        try {
          queryStream.hasNext();
        } catch (IllegalStateException e) {
          threw.countDown();
        }
      });
      t.setDaemon(true);
      t.start();

      // Let the thread run and then close the stream. Verify that it was interrupted
      Thread.sleep(100);
      queryStream.close();
      Assert.assertTrue(threw.await(10, TimeUnit.SECONDS));
      t.join(10000);
      Assert.assertFalse(t.isAlive());
    } finally {
      writers.get(0).finished();
    }
  }

  @Test
  public void testStatus() {
    RestResponse<CommandStatuses> commandStatusesRestResponse = ksqlRestClient.makeStatusRequest();
    Assert.assertNotNull(commandStatusesRestResponse);
    Assert.assertTrue(commandStatusesRestResponse.isSuccessful());
    CommandStatuses commandStatuses = commandStatusesRestResponse.getResponse();
    Assert.assertTrue(commandStatuses.size() == 2);
    Assert.assertTrue(commandStatuses.get(new CommandId(CommandId.Type.TOPIC, "c1", CommandId.Action.CREATE)) == CommandStatus.Status.SUCCESS);
    Assert.assertTrue(commandStatuses.get(new CommandId(CommandId.Type.TOPIC, "c2", CommandId.Action.CREATE)) ==
                      CommandStatus.Status.ERROR);

  }

}
