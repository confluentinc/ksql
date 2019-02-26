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

package io.confluent.ksql.rest.client;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.server.utils.TestUtils;
import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
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
import javax.ws.rs.core.Response.Status;
import org.apache.kafka.streams.StreamsConfig;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KsqlRestClientTest {

  private MockApplication mockApplication;
  private KsqlRestClient ksqlRestClient;

  @Before
  public void init() throws Exception {
    final int port = TestUtils.randomFreeLocalPort();
    final Map<String, Object> props = new HashMap<>();
    final String serverAddress = "http://localhost:" + port;
    props.put(KsqlRestConfig.LISTENERS_CONFIG, serverAddress);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ksql_config_test");
    final KsqlRestConfig ksqlRestConfig = new KsqlRestConfig(props);
    mockApplication = new MockApplication(ksqlRestConfig);
    mockApplication.start();

    ksqlRestClient = new KsqlRestClient(serverAddress);
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

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void shouldInterruptScannerOnClose() throws InterruptedException {
    final MockStreamedQueryResource sqr = mockApplication.getStreamedQueryResource();
    final RestResponse<KsqlRestClient.QueryStream> queryResponse = ksqlRestClient.makeQueryRequest
            ("Select *");
    Assert.assertNotNull(queryResponse);
    Assert.assertTrue(queryResponse.isSuccessful());

    final List<MockStreamedQueryResource.TestStreamWriter> writers = sqr.getWriters();
    Assert.assertEquals(1, writers.size());

          final KsqlRestClient.QueryStream queryStream = queryResponse.getResponse();

    final Thread closeThread = givenStreamWillBeClosedIn(Duration.ofMillis(100), queryStream);
    try {
      queryStream.hasNext();
      fail();
    } catch (final IllegalStateException e) {
      // expected
    }

    closeThread.join(10_000);
    assertThat("invalid test", closeThread.isAlive(), is(false));
  }

  @Test
  public void testStatus() {
    // When:
    final RestResponse<CommandStatuses> response = ksqlRestClient.makeStatusRequest();

    // Then:
    assertThat(response, is(notNullValue()));
    assertThat(response.isSuccessful(), is(true));
    assertThat(response.getResponse(), is(new CommandStatuses(ImmutableMap.of(
        new CommandId(CommandId.Type.TOPIC, "c1", CommandId.Action.CREATE),
        CommandStatus.Status.SUCCESS,
        new CommandId(CommandId.Type.TOPIC, "c2", CommandId.Action.CREATE),
        CommandStatus.Status.ERROR
    ))));
  }

  @Test
  public void shouldReturnStatusForSpecificCommand() {
    // When:
    final RestResponse<CommandStatus> response = ksqlRestClient.makeStatusRequest("TOPIC/c1/CREATE");

    // Then:
    assertThat(response, is(notNullValue()));
    assertThat(response.isSuccessful(), is(true));
    assertThat(response.getResponse().getStatus(), is(CommandStatus.Status.SUCCESS));
  }

  @Test(expected = KsqlRestClientException.class)
  public void shouldThrowOnInvalidServerAddress() {
    new KsqlRestClient("not-valid-address");
  }

  @Test
  public void shouldHandleNotFoundOnGetRequests() {
    // Given:
    givenServerWillReturn(Status.NOT_FOUND);

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(404));
    assertThat(response.getErrorMessage().getMessage(),
        containsString("Path not found. Path='/info'. "
            + "Check your ksql http url to make sure you are connecting to a ksql server."));
  }

  @Test
  public void shouldHandleNotFoundOnPostRequests() {
    // Given:
    givenServerWillReturn(Status.NOT_FOUND);

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva");

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(404));
    assertThat(response.getErrorMessage().getMessage(),
        containsString("Path not found. Path='ksql'. "
            + "Check your ksql http url to make sure you are connecting to a ksql server."));
  }

  @Test
  public void shouldHandleUnauthorizedOnGetRequests() {
    // Given:
    givenServerWillReturn(Status.UNAUTHORIZED);

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(Errors.ERROR_CODE_UNAUTHORIZED));
    assertThat(response.getErrorMessage().getMessage(),
        is("Could not authenticate successfully with the supplied credentials."));
  }

  @Test
  public void shouldHandleUnauthorizedOnPostRequests() {
    // Given:
    givenServerWillReturn(Status.UNAUTHORIZED);

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva");

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(Errors.ERROR_CODE_UNAUTHORIZED));
    assertThat(response.getErrorMessage().getMessage(),
        is("Could not authenticate successfully with the supplied credentials."));
  }

  @Test
  public void shouldHandleForbiddendOnGetRequests() {
    // Given:
    givenServerWillReturn(Status.FORBIDDEN);

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(Errors.ERROR_CODE_FORBIDDEN));
    assertThat(response.getErrorMessage().getMessage(),
        is("You are forbidden from using this cluster."));
  }

  @Test
  public void shouldHandleForbiddenOnPostRequests() {
    // Given:
    givenServerWillReturn(Status.FORBIDDEN);

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva");

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(Errors.ERROR_CODE_FORBIDDEN));
    assertThat(response.getErrorMessage().getMessage(),
        is("You are forbidden from using this cluster."));
  }

  @Test
  public void shouldHandleErrorMessageOnGetRequests() {
    // Given:
    givenServerWillReturn(new KsqlErrorMessage(12300, "ouch", ImmutableList.of("s1", "s2")));

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(12300));
    assertThat(response.getErrorMessage().getMessage(), is("ouch"));
    assertThat(response.getErrorMessage().getStackTrace(), is(ImmutableList.of("s1", "s2")));
  }

  @Test
  public void shouldHandleErrorMessageOnPostRequests() {
    // Given:
    givenServerWillReturn(new KsqlErrorMessage(12300, "ouch", ImmutableList.of("s1", "s2")));

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva");

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(12300));
    assertThat(response.getErrorMessage().getMessage(), is("ouch"));
    assertThat(response.getErrorMessage().getStackTrace(), is(ImmutableList.of("s1", "s2")));
  }

  @Test
  public void shouldHandleArbitraryErrorsOnGetRequests() {
    // Given:
    givenServerWillReturn(Status.EXPECTATION_FAILED);

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(),
        is(Errors.toErrorCode(Status.EXPECTATION_FAILED.getStatusCode())));
    assertThat(response.getErrorMessage().getMessage(),
        is("The server returned an unexpected error."));
  }

  @Test
  public void shouldHandleArbitraryErrorsOnPostRequests() {
    // Given:
    givenServerWillReturn(Status.EXPECTATION_FAILED);

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva");

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(),
        is(Errors.toErrorCode(Status.EXPECTATION_FAILED.getStatusCode())));
    assertThat(response.getErrorMessage().getMessage(),
        is("The server returned an unexpected error."));
  }

  @Test
  public void shouldHandleSuccessOnGetRequests() {
    // Given:
    final ServerInfo expectedEntity = new ServerInfo("1", "cid", "sid");
    givenServerWillReturn(expectedEntity);

    // When:
    final RestResponse<ServerInfo> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.get(), is(expectedEntity));
  }

  @Test
  public void shouldHandleSuccessOnPostRequests() {
    // Given:
    final KsqlEntityList expectedEntity = new KsqlEntityList();
    givenServerWillReturn(expectedEntity);

    // When:
    final RestResponse<KsqlEntityList> response = ksqlRestClient.makeKsqlRequest("foo");

    // Then:
    assertThat(response.get(), is(expectedEntity));
  }

  private void givenServerWillReturn(final KsqlErrorMessage errorMessage) {
    final int statusCode = Errors.toStatusCode(errorMessage.getErrorCode());
    givenServerWillReturn(statusCode, Optional.of(errorMessage));
  }

  private void givenServerWillReturn(final Status statusCode) {
    givenServerWillReturn(statusCode.getStatusCode(), Optional.empty());
  }

  private void givenServerWillReturn(final Object entity) {
    givenServerWillReturn(Status.OK.getStatusCode(), Optional.of(entity));
  }

  @SuppressWarnings("unchecked")
  private <T> void givenServerWillReturn(final int statusCode, final Optional<T> entity) {

    final Response response = EasyMock.createNiceMock(Response.class);
    EasyMock.expect(response.getStatus()).andReturn(statusCode).anyTimes();
    entity.ifPresent(e ->
        EasyMock.expect(response.readEntity((Class<T>)e.getClass())).andReturn(e).once());

    final Invocation.Builder builder = EasyMock.createNiceMock(Invocation.Builder.class);
    EasyMock.expect(builder.get()).andReturn(response);
    EasyMock.expect(builder.post(EasyMock.anyObject())).andReturn(response);

    final WebTarget target = EasyMock.createNiceMock(WebTarget.class);
    EasyMock.expect(target.path(EasyMock.anyString())).andReturn(target);
    EasyMock.expect(target.request(MediaType.APPLICATION_JSON_TYPE)).andReturn(builder);

    final Client client = EasyMock.createNiceMock(Client.class);
    EasyMock.expect(client.target(EasyMock.anyObject(URI.class))).andReturn(target);

    EasyMock.replay(client, target, builder, response);

    ksqlRestClient = new KsqlRestClient(client, "http://0.0.0.0", Collections.emptyMap());
  }

  private static Thread givenStreamWillBeClosedIn(final Duration duration, final Closeable stream) {
    final Thread thread = new Thread(() -> {
      try {
        Thread.sleep(duration.toMillis());
        stream.close();
      } catch (final Exception e) {
        // Meh
      }
    });
    thread.setDaemon(true);
    thread.start();
    return thread;
  }
}
