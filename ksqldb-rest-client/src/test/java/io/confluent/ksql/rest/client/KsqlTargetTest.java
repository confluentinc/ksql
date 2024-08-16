package io.confluent.ksql.rest.client;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.streams.WriteStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlTargetTest {

  private static final String HOST = "host";
  private static final String QUERY = "SELECT * from RATINGS_TABLE;";

  @Mock
  private HttpClient httpClient;
  @Mock
  private SocketAddress socketAddress;
  @Mock
  private LocalProperties localProperties;
  @Mock
  private Optional<String> authHeader;
  @Mock
  private HttpClientRequest httpClientRequest;
  @Mock
  private HttpClientResponse httpClientResponse;
  @Mock
  private HttpConnection httpConnection;
  @Mock
  private WriteStream<List<StreamedRow>> writeStream;
  @Captor
  private ArgumentCaptor<Handler<Buffer>> handlerCaptor;
  @Captor
  private ArgumentCaptor<Handler<Void>> endCaptor;
  @Captor
  private ArgumentCaptor<Handler<Throwable>> exceptionCaptor;

  private CompletableFuture<Void> closeConnection;
  private Vertx vertx;
  private KsqlTarget ksqlTarget;
  private ExecutorService executor;
  private AtomicBoolean requestStarted = new AtomicBoolean();
  private AtomicReference<RestResponse<Integer>> response = new AtomicReference<>();
  private CopyOnWriteArrayList<StreamedRow> rows = new CopyOnWriteArrayList<>();
  private AtomicReference<Throwable> error = new AtomicReference<>();

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    closeConnection = new CompletableFuture<>();
    executor = Executors.newSingleThreadExecutor();

    when(httpClientRequest.response(any(Handler.class))).thenAnswer(a -> {
      final Handler<AsyncResult<HttpClientResponse>> handler = a.getArgument(0);
      vertx.runOnContext(v -> {
        handler.handle(Future.succeededFuture(httpClientResponse));
        requestStarted.set(true);
      });
      return null;
    });
    doAnswer(a -> {
      final Handler<AsyncResult<HttpClientRequest>> handler = a.getArgument(1);
      handler.handle(Future.succeededFuture(httpClientRequest));
      return null;
    }).when(httpClient).request(any(RequestOptions.class), any(Handler.class));

    when(httpClientResponse.handler(handlerCaptor.capture()))
        .thenReturn(httpClientResponse);
    when(httpClientResponse.bodyHandler(handlerCaptor.capture()))
        .thenReturn(httpClientResponse);
    when(httpClientResponse.endHandler(endCaptor.capture()))
        .thenReturn(httpClientResponse);
    when(httpClientResponse.statusCode()).thenReturn(OK.code());
    when(httpClientResponse.request()).thenReturn(httpClientRequest);
    when(httpClientRequest.exceptionHandler(exceptionCaptor.capture()))
        .thenReturn(httpClientRequest);
    when(httpClientRequest.connection())
        .thenReturn(httpConnection);

    error.set(null);
  }

  @After
  public void tearDown() {
    vertx.close();
    executor.shutdownNow();
  }

  private void expectPostQueryRequestChunkHandler() {
    try {
      doAnswer(inv -> {
        final List<StreamedRow> rs = inv.getArgument(0);
        if (rs != null) {
          rows.addAll(rs);
        }
        return writeStream;
      }).when(writeStream).write(any(), any());

      response.set(ksqlTarget.postQueryRequest(
          QUERY, ImmutableMap.of(), Optional.empty(), writeStream, closeConnection, Function.identity()));
    } catch (Throwable t) {
      error.set(t);
    }
  }

  @Test
  public void shouldPostQueryRequest_chunkHandler() {
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, Collections.emptyMap());
    executor.submit(this::expectPostQueryRequestChunkHandler);
    assertThatEventually(requestStarted::get, is(true));

    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [1.0, 12.1]}},\n"));
    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [5.0, 10.5]}},\n"));
    endCaptor.getValue().handle(null);

    assertThatEventually(response::get, notNullValue());
    assertThat(response.get().getResponse(), is (2));
    assertThat(rows.size(), is (2));
  }

  @Test
  public void shouldPostQueryRequest_chunkHandler_exception() {
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, Collections.emptyMap());
    executor.submit(this::expectPostQueryRequestChunkHandler);

    assertThatEventually(requestStarted::get, is(true));

    exceptionCaptor.getValue().handle(new RuntimeException("Error!"));

    assertThatEventually(error::get, notNullValue());
    assertThat(error.get().getMessage(),
        containsString("Error issuing POST to KSQL server. path:/query"));
  }

  @Test
  public void shouldPostQueryRequest_chunkHandler_closeEarly() {
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, Collections.emptyMap());
    executor.submit(this::expectPostQueryRequestChunkHandler);

    assertThatEventually(requestStarted::get, is(true));

    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [1.0, 12.1]}},\n"));
    closeConnection.complete(null);

    assertThatEventually(error::get, notNullValue());
    assertThat(error.get().getMessage(),
        containsString("Error issuing POST to KSQL server. path:/query"));
    assertThat(rows.size(), is (1));
  }

  @Test
  public void shouldPostQueryRequest_chunkHandler_closeEarlyWithError() {
    doThrow(new RuntimeException("Error!")).when(httpConnection).close();
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, Collections.emptyMap());
    executor.submit(this::expectPostQueryRequestChunkHandler);

    assertThatEventually(requestStarted::get, is(true));

    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [1.0, 12.1]}},\n"));
    closeConnection.complete(null);

    assertThatEventually(error::get, notNullValue());
    assertThat(error.get().getMessage(),
        containsString("Error issuing POST to KSQL server. path:/query"));
    assertThat(rows.size(), is (1));
  }

  @Test
  public void shouldPostQueryRequest_chunkHandler_closeAfterFinish() {
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, Collections.emptyMap());
    executor.submit(this::expectPostQueryRequestChunkHandler);

    assertThatEventually(requestStarted::get, is(true));

    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [1.0, 12.1]}},\n"));
    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [5.0, 10.5]}},\n"));
    endCaptor.getValue().handle(null);
    closeConnection.complete(null);

    assertThatEventually(response::get, notNullValue());
    assertThat(response.get().getResponse(), is (2));
    assertThat(rows.size(), is (2));
  }

  @Test
  public void shouldPostQueryRequest_chunkHandler_partialMessage() {
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, Collections.emptyMap());
    executor.submit(this::expectPostQueryRequestChunkHandler);

    assertThatEventually(requestStarted::get, is(true));

    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\": [1.0, 12.1]}},\n"));
    handlerCaptor.getValue().handle(Buffer.buffer("{\"row\": {\"columns\""));
    handlerCaptor.getValue().handle(Buffer.buffer(": [5.0, 10.5]}},\n"));
    endCaptor.getValue().handle(null);
    closeConnection.complete(null);

    assertThatEventually(response::get, notNullValue());
    assertThat(response.get().getResponse(), is (2));
    assertThat(rows.size(), is (2));
  }

  @Test
  public void shouldSendAdditionalHeadersWithKsqlRequest() {
    // Given:
    final Map<String, String> additionalHeaders = ImmutableMap.of("h1", "v1", "h2", "v2");
    ksqlTarget = new KsqlTarget(httpClient, socketAddress, localProperties, authHeader, HOST, additionalHeaders);

    // When:
    executor.submit(() -> {
      try {
        ksqlTarget.postKsqlRequest("some ksql;", Collections.emptyMap(), Optional.empty());
      } catch (Exception e) {
        // ignore response error since this test is just testing headers on the outgoing request
      }
    });
    assertThatEventually(requestStarted::get, is(true));
    handlerCaptor.getValue().handle(Buffer.buffer());

    // Then:
    verify(httpClientRequest).putHeader("h1", "v1");
    verify(httpClientRequest).putHeader("h2", "v2");
  }
}
