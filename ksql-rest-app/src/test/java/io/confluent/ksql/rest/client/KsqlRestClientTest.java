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

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.rest.server.mock.MockApplication;
import io.confluent.ksql.rest.server.mock.MockStreamedQueryResource;
import io.confluent.ksql.rest.server.utils.TestUtils;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KsqlRestClientTest {

  MockApplication mockApplication;
  KsqlRestConfig ksqlRestConfig;
  KsqlRestClient ksqlRestClient;

  @Before
  public void init() throws Exception {
    final int port = TestUtils.randomFreeLocalPort();
    final Map<String, Object> props = new HashMap<>();
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
    final RestResponse<KsqlEntityList> results = ksqlRestClient.makeKsqlRequest("Test request");
    Assert.assertNotNull(results);
    Assert.assertTrue(results.isSuccessful());
    final KsqlEntityList ksqlEntityList = results.getResponse();
    Assert.assertTrue(ksqlEntityList.size() == 1);
    Assert.assertTrue(ksqlEntityList.get(0) instanceof ExecutionPlan);
  }


  @Test
  public void testStreamRowFromServer() throws InterruptedException {
    final MockStreamedQueryResource sqr = mockApplication.getStreamedQueryResource();
    final RestResponse<KsqlRestClient.QueryStream> queryResponse = ksqlRestClient.makeQueryRequest
            ("Select *");
    Assert.assertNotNull(queryResponse);
    Assert.assertTrue(queryResponse.isSuccessful());

    // Get the stream writer from the mock server and load it up with a row
    final List<MockStreamedQueryResource.TestStreamWriter> writers = sqr.getWriters();
    Assert.assertEquals(1, writers.size());
    final MockStreamedQueryResource.TestStreamWriter writer = writers.get(0);
    try {
      writer.enq("hello");

      // Try and receive the row. Do this from another thread to avoid blocking indefinitely
      final KsqlRestClient.QueryStream queryStream = queryResponse.getResponse();
      final Thread t = new Thread(() -> queryStream.hasNext());
      t.setDaemon(true);
      t.start();
      t.join(10000);
      Assert.assertFalse(t.isAlive());
      Assert.assertTrue(queryStream.hasNext());

      final StreamedRow sr = queryStream.next();
      Assert.assertNotNull(sr);
      final GenericRow row = sr.getRow();
      Assert.assertEquals(1, row.getColumns().
              size());
      Assert.assertEquals("hello", row.getColumns().
              get(0));
      writer.enq("{\"row\":null,\"errorMessage\":null,\"finalMessage\":\"Limit Reached\"}");
      final Thread t1 = new Thread(() -> queryStream.hasNext());
      t1.setDaemon(true);
      t1.start();
      t1.join(10000);
      Assert.assertFalse(t1.isAlive());
      Assert.assertTrue(queryStream.hasNext());
      final StreamedRow error_message = queryStream.next();
      System.out.println();

    } finally {
      writer.finished();
    }
  }

  @Test
  public void shouldInterruptScannerOnClose() throws InterruptedException {
    final MockStreamedQueryResource sqr = mockApplication.getStreamedQueryResource();
    final RestResponse<KsqlRestClient.QueryStream> queryResponse = ksqlRestClient.makeQueryRequest
            ("Select *");
    Assert.assertNotNull(queryResponse);
    Assert.assertTrue(queryResponse.isSuccessful());

    final List<MockStreamedQueryResource.TestStreamWriter> writers = sqr.getWriters();
    Assert.assertEquals(1, writers.size());
    try {
      // Try and receive a row. This will block since there is no data to return
      final KsqlRestClient.QueryStream queryStream = queryResponse.getResponse();
      final CountDownLatch threw = new CountDownLatch(1);
      final Thread t = new Thread(() -> {
        try {
          queryStream.hasNext();
        } catch (final IllegalStateException e) {
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
    final RestResponse<CommandStatuses> commandStatusesRestResponse = ksqlRestClient.makeStatusRequest();
    Assert.assertNotNull(commandStatusesRestResponse);
    Assert.assertTrue(commandStatusesRestResponse.isSuccessful());
    final CommandStatuses commandStatuses = commandStatusesRestResponse.getResponse();
    Assert.assertTrue(commandStatuses.size() == 2);
    Assert.assertTrue(commandStatuses.get(new CommandId(CommandId.Type.TOPIC, "c1", CommandId.Action.CREATE)) == CommandStatus.Status.SUCCESS);
    Assert.assertTrue(commandStatuses.get(new CommandId(CommandId.Type.TOPIC, "c2", CommandId.Action.CREATE)) ==
                      CommandStatus.Status.ERROR);

  }

  @Test
  public void shouldReturnStatusForSpecificCommand() {
    final RestResponse<CommandStatus> commandStatusRestResponse = ksqlRestClient.makeStatusRequest("TOPIC/c1/CREATE");
    Assert.assertThat(commandStatusRestResponse, CoreMatchers.notNullValue());
    Assert.assertThat(commandStatusRestResponse.isSuccessful(), CoreMatchers.equalTo(true));
    final CommandStatus commandStatus = commandStatusRestResponse.getResponse();
    Assert.assertThat(commandStatus.getStatus(), CoreMatchers.equalTo(CommandStatus.Status.SUCCESS));
  }

  @Test(expected = KsqlRestClientException.class)
  public void shouldThrowOnInvalidServerAddress() {
    new KsqlRestClient("not-valid-address", Collections.emptyMap());
  }

  private <T> Client mockClientExpectingGetRequestAndReturningStatusWithEntity(
      final String server, final String path, final Response.Status status, final Optional<T> entity, final Class<T> clazz)
      throws Exception {
    final Client client = EasyMock.createNiceMock(Client.class);
    final WebTarget target = EasyMock.createNiceMock(WebTarget.class);

    EasyMock.expect(client.target(new URI(server))).andReturn(target);
    EasyMock.expect(target.path(path)).andReturn(target);
    final Invocation.Builder builder = EasyMock.createNiceMock(Invocation.Builder.class);
    EasyMock.expect(target.request(MediaType.APPLICATION_JSON_TYPE)).andReturn(builder);
    final Response response = EasyMock.createNiceMock(Response.class);
    EasyMock.expect(builder.get()).andReturn(response);
    EasyMock.expect(response.getStatus()).andReturn(status.getStatusCode()).anyTimes();
    if (entity.isPresent()) {
      EasyMock.expect(response.readEntity(clazz)).andReturn(entity.get()).anyTimes();
    }
    EasyMock.replay(client, target, builder, response);

    return client;
  }

  private Client mockClientExpectingGetRequestAndReturningStatus(
      final String server, final String path, final Response.Status status) throws Exception {
    return mockClientExpectingGetRequestAndReturningStatusWithEntity(
        server, path, status, Optional.empty(), Object.class);
  }

  @Test
  public void shouldRaiseAuthenticationExceptionOn401Response() throws Exception {
    final String serverAddress = "http://foobar";
    final Client client = mockClientExpectingGetRequestAndReturningStatus(
        serverAddress, "/info", Response.Status.UNAUTHORIZED);
    final KsqlRestClient restClient = new KsqlRestClient(client, serverAddress, Collections.emptyMap());
    final RestResponse restResponse = restClient.getServerInfo();
    assertTrue(restResponse.isErroneous());
  }

  @Test
  public void shouldReturnSuccessfulResponseWhenAuthenticationSucceeds() throws Exception {
    final String serverAddress = "http://foobar";
    final Client client = mockClientExpectingGetRequestAndReturningStatus(
        serverAddress, "/info", Response.Status.OK);
    final KsqlRestClient restClient = new KsqlRestClient(client, serverAddress, Collections.emptyMap());
    final RestResponse restResponse = restClient.getServerInfo();
    assertTrue(restResponse.isSuccessful());
  }

  @Test
  public void shouldReturnErroneousResponseOnError() throws Exception {
    final String serverAddress = "http://foobar";
    final KsqlErrorMessage ksqlError = new KsqlErrorMessage(500001, "badbadnotgood");
    final Client mockClient = mockClientExpectingGetRequestAndReturningStatusWithEntity(
        serverAddress, "/info", Response.Status.INTERNAL_SERVER_ERROR,
        Optional.of(ksqlError), KsqlErrorMessage.class);
    final KsqlRestClient ksqlRestClient = new KsqlRestClient(mockClient, serverAddress, Collections.emptyMap());
    final RestResponse restResponse = ksqlRestClient.makeRequest("/info", ServerInfo.class);
    assertThat(restResponse.isErroneous(), CoreMatchers.equalTo(true));
    assertThat(restResponse.getErrorMessage(), CoreMatchers.equalTo(ksqlError));
  }
}
