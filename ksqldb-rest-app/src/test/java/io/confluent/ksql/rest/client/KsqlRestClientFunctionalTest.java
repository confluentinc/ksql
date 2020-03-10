/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.ExecutionPlan;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.ExecutableServer;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.mock.MockApplication;
import io.confluent.ksql.rest.server.mock.MockStreamedQueryResource;
import io.confluent.ksql.rest.server.mock.MockStreamedQueryResource.TestStreamWriter;
import io.confluent.rest.ApplicationServer;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.http.HttpStatus;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestClientFunctionalTest {

  private MockApplication mockApplication;
  private ExecutableServer<KsqlRestConfig> mockServer;
  private KsqlRestClient ksqlRestClient;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void init() throws Exception {
    final Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:0")
        .put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        .put(RestConfig.SHUTDOWN_GRACEFUL_MS_CONFIG, (int) TimeUnit.SECONDS.toMillis(30))
        .build();

    final KsqlRestConfig ksqlRestConfig = new KsqlRestConfig(props);
    mockApplication = new MockApplication(ksqlRestConfig);
    mockServer = new ExecutableServer<>(
        new ApplicationServer<>(ksqlRestConfig),
        ImmutableList.of(mockApplication)
    );

    mockServer.startAsync();

    ksqlRestClient = buildClient(mockApplication.getServerAddress());
  }

  @After
  public void cleanUp() {
    try {
      mockServer.triggerShutdown();
    } catch (final Exception e) {
      System.err.println("Failed to stop app");
      e.printStackTrace(System.err);
    }
  }

  @Test
  public void testKsqlResource() {
    final RestResponse<KsqlEntityList> results =
        ksqlRestClient.makeKsqlRequest("Test request", null);

    assertThat(results, is(notNullValue()));
    assertThat(results.isSuccessful(), is(true));

    final KsqlEntityList ksqlEntityList = results.getResponse();
    assertThat(ksqlEntityList, hasSize(1));
    assertThat(ksqlEntityList.get(0), is(instanceOf(ExecutionPlan.class)));
  }

  @Test
  public void testStreamRowFromServer() throws InterruptedException {
    // Given:
    final RestResponse<QueryStream> queryResponse =
        ksqlRestClient.makeQueryRequest("Select *", null);

    final ReceiverThread receiver = new ReceiverThread(queryResponse);

    final MockStreamedQueryResource.TestStreamWriter writer = getResponseWriter();

    // When:
    writer.enq("hello");
    writer.enq("world");
    writer.enq("{\"row\":null,\"errorMessage\":null,\"finalMessage\":\"Limit Reached\"}");
    writer.finished();

    // Then:
    assertThat(receiver.getRows(), contains(
        StreamedRow.row(genericRow("hello")),
        StreamedRow.row(genericRow("world")),
        StreamedRow.finalMessage("Limit Reached")));
  }

  @Test
  public void shouldHandleSlowResponsesFromServer() throws InterruptedException {
    // Given:
    givenResponsesDelayedBy(Duration.ofSeconds(3));

    final RestResponse<QueryStream> queryResponse =
        ksqlRestClient.makeQueryRequest("Select *", null);

    final ReceiverThread receiver = new ReceiverThread(queryResponse);

    final MockStreamedQueryResource.TestStreamWriter writer = getResponseWriter();

    // When:
    writer.enq("hello");
    writer.enq("world");
    writer.enq("{\"row\":null,\"errorMessage\":null,\"finalMessage\":\"Limit Reached\"}");
    writer.finished();

    // Then:
    assertThat(receiver.getRows(), contains(
        StreamedRow.row(genericRow("hello")),
        StreamedRow.row(genericRow("world")),
        StreamedRow.finalMessage("Limit Reached")));
  }

  @Test
  public void shouldReturnFalseFromHasNextIfClosedAsynchronously() throws Exception {
    // Given:
    final RestResponse<QueryStream> queryResponse =
        ksqlRestClient.makeQueryRequest("Select *", null);

    final QueryStream stream = queryResponse.getResponse();

    final Thread closeThread = givenStreamWillCloseIn(Duration.ofMillis(500), stream);

    // When:
    final boolean result = stream.hasNext();

    // Then:
    assertThat(result, is(false));
    closeThread.join(1_000);
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
    buildClient("not-valid-address");
  }

  @Test
  public void shouldParseSingleServerAddress() throws Exception {
    final String singleServerAddress = "http://singleServer:8088";
    final URI singleServerURI = new URI (singleServerAddress);
    try (KsqlRestClient client = buildClient(singleServerAddress)) {
      assertThat(client.getServerAddress(), is(singleServerURI));
    }
  }

  @Test
  public void shouldParseMultipleServerAddresses() throws Exception {
    final String firstServerAddress = "http://firstServer:8088";
    final String multipleServerAddresses = firstServerAddress + ",http://secondServer:8088";
    final URI firstServerURI = new URI (firstServerAddress);
    try (KsqlRestClient client = buildClient(multipleServerAddresses)) {
      assertThat(client.getServerAddress(), is(firstServerURI));
    }
  }

  @Test
  public void shouldThrowIfAnyServerAddressIsInvalid() {
    expectedException.expect(KsqlRestClientException.class);
    expectedException.expectMessage("The supplied serverAddress is invalid: secondBuggyServer.8088");
    buildClient("http://firstServer:8088,secondBuggyServer.8088");
  }

  @Test
  public void shouldHandleNotFoundOnGetRequests() {
    // Given:
    givenServerWillReturn(Status.NOT_FOUND);

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getStatusCode().getCode(), is(HttpStatus.SC_NOT_FOUND));
    assertThat(response.getErrorMessage().getErrorCode(), is(40400));
    assertThat(response.getErrorMessage().getMessage(),
        containsString("Path not found. Path='/info'. "
            + "Check your ksql http url to make sure you are connecting to a ksql server."));
  }

  @Test
  public void shouldHandleNotFoundOnPostRequests() {
    // Given:
    givenServerWillReturn(Status.NOT_FOUND);

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva", null);

    // Then:
    assertThat(response.getStatusCode().getCode(), is(HttpStatus.SC_NOT_FOUND));
    assertThat(response.getErrorMessage().getErrorCode(), is(40400));
    assertThat(response.getErrorMessage().getMessage(),
        containsString("Path not found. Path='/ksql'. "
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
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva", null);

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(Errors.ERROR_CODE_UNAUTHORIZED));
    assertThat(response.getErrorMessage().getMessage(),
        is("Could not authenticate successfully with the supplied credentials."));
  }

  @Test
  public void shouldHandleForbiddenOnGetRequests() {
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
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva", null);

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(), is(Errors.ERROR_CODE_FORBIDDEN));
    assertThat(response.getErrorMessage().getMessage(),
        is("You are forbidden from using this cluster."));
  }

  @Test
  public void shouldHandleErrorMessageOnGetRequests() {
    // Given:
    givenServerWillReturn(new KsqlErrorMessage(40000, "ouch", ImmutableList.of("s1", "s2")));

    // When:
    final RestResponse<?> response = ksqlRestClient.getServerInfo();

    // Then:
    assertThat(response.getStatusCode().getCode(), is(HttpStatus.SC_BAD_REQUEST));
    assertThat(response.getErrorMessage().getErrorCode(), is(40000));
    assertThat(response.getErrorMessage().getMessage(), is("ouch"));
    assertThat(response.getErrorMessage().getStackTrace(), is(ImmutableList.of("s1", "s2")));
  }

  @Test
  public void shouldHandleErrorMessageOnPostRequests() {
    // Given:
    givenServerWillReturn(new KsqlErrorMessage(40100, "ouch", ImmutableList.of("s1", "s2")));

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva", null);

    // Then:
    assertThat(response.getStatusCode().getCode(), is(HttpStatus.SC_UNAUTHORIZED));
    assertThat(response.getErrorMessage().getErrorCode(), is(40100));
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
        is("The server returned an unexpected error: Expectation Failed"));
  }

  @Test
  public void shouldHandleArbitraryErrorsOnPostRequests() {
    // Given:
    givenServerWillReturn(Status.EXPECTATION_FAILED);

    // When:
    final RestResponse<?> response = ksqlRestClient.makeKsqlRequest("whateva", null);

    // Then:
    assertThat(response.getErrorMessage().getErrorCode(),
        is(Errors.toErrorCode(Status.EXPECTATION_FAILED.getStatusCode())));
    assertThat(response.getErrorMessage().getMessage(),
        is("The server returned an unexpected error: Expectation Failed"));
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
    final RestResponse<KsqlEntityList> response = ksqlRestClient.makeKsqlRequest("foo", null);

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
    final Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(statusCode);
    when(response.getStatusInfo()).thenReturn(Status.fromStatusCode(statusCode));

    entity.ifPresent(e -> when(response.readEntity((Class<T>) e.getClass())).thenReturn(e));

    final Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.headers(any())).thenReturn(builder);
    when(builder.property(any(), any())).thenReturn(builder);
    when(builder.get()).thenReturn(response);
    when(builder.post(any())).thenReturn(response);

    final WebTarget target = mock(WebTarget.class);
    when(target.path(any())).thenReturn(target);
    when(target.request(MediaType.APPLICATION_JSON_TYPE)).thenReturn(builder);

    final Client client = mock(Client.class);
    when(client.target(any(URI.class))).thenReturn(target);

    final LocalProperties localProps = new LocalProperties(ImmutableMap.of());
    final KsqlClient ksqlClient = new KsqlClient(client, Optional.empty(), localProps);
    ksqlRestClient = new KsqlRestClient(ksqlClient, "http://0.0.0.0", localProps);
  }

  private void givenResponsesDelayedBy(final Duration delay) {
    mockApplication.getStreamedQueryResource().setResponseDelay(delay.toMillis());
  }

  private TestStreamWriter getResponseWriter() {
    final MockStreamedQueryResource sqr = mockApplication.getStreamedQueryResource();
    // There can be multiple writers, due to some requests timing out and retrying.
    // The last is the one we want:
    return Iterables.getLast(sqr.getWriters());
  }

  private static Thread givenStreamWillCloseIn(final Duration duration, final QueryStream stream) {
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

  private static KsqlRestClient buildClient(final String serverAddress) {
    return KsqlRestClient.create(
        serverAddress,
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.empty()
    );
  }

  private static final class ReceiverThread {

    private final QueryStream queryStream;
    private final List<StreamedRow> rows = new CopyOnWriteArrayList<>();
    private final AtomicReference<Exception> exception = new AtomicReference<>();
    private final Thread thread;

    private ReceiverThread(final RestResponse<QueryStream> queryResponse) {
      assertThat("not successful", queryResponse.isSuccessful(), is(true));
      this.queryStream = queryResponse.getResponse();
      this.thread = new Thread(() -> {
        try {
          while (queryStream.hasNext()) {
            final StreamedRow row = queryStream.next();
            rows.add(row);
          }

        } catch (final Exception e) {
          exception.set(e);
        }
      }, "receiver-thread");
      thread.setDaemon(true);
      thread.start();
    }

    private List<StreamedRow> getRows() throws InterruptedException {
      thread.join(20_000);
      assertThat("Receive thread still running", thread.isAlive(),  is(false));
      if (exception.get() != null) {
        throw new RuntimeException(exception.get());
      }
      return rows;
    }
  }
}
