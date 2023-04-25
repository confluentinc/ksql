package io.confluent.ksql.physical.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.scalablepush.PushRouting.PushConnectionsHandle;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator;
import io.confluent.ksql.physical.scalablepush.locator.PushLocator.KsqlNode;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PushRoutingTest {

  private static final List<?> LOCAL_ROW1 = ImmutableList.of(1, "a");
  private static final List<?> LOCAL_ROW2 = ImmutableList.of(2, "b");

  private static final StreamedRow REMOTE_ROW1
      = StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of(3, "c")));
  private static final StreamedRow REMOTE_ROW2
      = StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of(4, "d")));

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SimpleKsqlClient simpleKsqlClient;
  @Mock
  private PushPhysicalPlan pushPhysicalPlan;
  @Mock
  private ConfiguredStatement<Query> statement;
  @Mock
  private SessionConfig sessionConfig;
  @Mock
  private PushRoutingOptions pushRoutingOptions;
  @Mock
  private LogicalSchema outputSchema;
  @Mock
  private ScalablePushRegistry scalablePushRegistry;
  @Mock
  private PushLocator locator;
  @Mock
  private KsqlNode ksqlNodeLocal;
  @Mock
  private KsqlNode ksqlNodeRemote;
  @Mock
  private TransientQueryQueue transientQueryQueueMock;

  private Vertx vertx;
  private Context context;
  private TransientQueryQueue transientQueryQueue;


  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    when(statement.getMaskedStatementText()).thenReturn("SELECT * FROM STREAM EMIT CHANGES");
    when(statement.getUnMaskedStatementText()).thenReturn("SELECT * FROM STREAM EMIT CHANGES");
    when(statement.getSessionConfig()).thenReturn(sessionConfig);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of());
    when(serviceContext.getKsqlClient()).thenReturn(simpleKsqlClient);
    when(pushPhysicalPlan.getScalablePushRegistry()).thenReturn(scalablePushRegistry);
    when(scalablePushRegistry.getLocator()).thenReturn(locator);
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeLocal, ksqlNodeRemote));
    when(ksqlNodeLocal.location()).thenReturn(URI.create("http://localhost:8088"));
    when(ksqlNodeLocal.isLocal()).thenReturn(true);
    when(ksqlNodeRemote.location()).thenReturn(URI.create("http://remote:8088"));
    when(ksqlNodeRemote.isLocal()).thenReturn(false);
    when(pushRoutingOptions.getIsSkipForwardRequest()).thenReturn(false);

    transientQueryQueue = new TransientQueryQueue(OptionalInt.empty());
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  private static <T> CompletableFuture<T> createFuture(T returnValue) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.complete(returnValue);
    return future;
  }

  private static <T> CompletableFuture<T> createErrorFuture(Throwable t) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  @Test
  public void shouldSucceed_forward() throws ExecutionException, InterruptedException {
    // Given:
    final PushRouting routing = new PushRouting();
    BufferedPublisher<List<?>> localPublisher = new BufferedPublisher<>(context);
    BufferedPublisher<StreamedRow> remotePublisher = new BufferedPublisher<>(context);
    when(pushPhysicalPlan.execute()).thenReturn(localPublisher);
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenReturn(createFuture(RestResponse.successful(200, remotePublisher)));

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    future.get();
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
    });

    // Then:
    Set<List<?>> rows = new HashSet<>();
    while (rows.size() < 4) {
      final KeyValue<List<?>, GenericRow> kv = transientQueryQueue.poll();
      if (kv == null) {
        Thread.sleep(100);
        continue;
      }
      rows.add(kv.value().values());
    }
    assertThat(rows.contains(LOCAL_ROW1), is(true));
    assertThat(rows.contains(LOCAL_ROW2), is(true));
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
  }

  @Test
  public void shouldSucceed_justForwarded() throws ExecutionException, InterruptedException {
    // Given:
    when(pushRoutingOptions.getIsSkipForwardRequest()).thenReturn(true);
    final PushRouting routing = new PushRouting();
    BufferedPublisher<List<?>> localPublisher = new BufferedPublisher<>(context);
    when(pushPhysicalPlan.execute()).thenReturn(localPublisher);

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    future.get();
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
    });

    // Then:
    verify(simpleKsqlClient, never()).makeQueryRequestStreamed(any(), any(), any(), any());
    Set<List<?>> rows = new HashSet<>();
    while (rows.size() < 2) {
      final KeyValue<List<?>, GenericRow> kv = transientQueryQueue.poll();
      if (kv == null) {
        Thread.sleep(100);
        continue;
      }
      rows.add(kv.value().values());
    }
    assertThat(rows.contains(LOCAL_ROW1), is(true));
    assertThat(rows.contains(LOCAL_ROW2), is(true));
  }

  @Test
  public void shouldFail_duringPlanExecute() throws ExecutionException, InterruptedException {
    // Given:
    when(pushRoutingOptions.getIsSkipForwardRequest()).thenReturn(true);
    final PushRouting routing = new PushRouting();
    when(pushPhysicalPlan.execute()).thenThrow(new RuntimeException("Error!"));

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    PushConnectionsHandle handle = future.get();

    // Then:
    assertThat(handle.getError().getMessage(), containsString("Error!"));
  }

  @Test
  public void shouldFail_non200RemoteCall() throws ExecutionException, InterruptedException {
    // Given:
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeRemote));
    final PushRouting routing = new PushRouting();
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenReturn(createFuture(RestResponse.erroneous(500, "Error response!")));

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    PushConnectionsHandle handle = future.get();

    // Then:
    assertThat(handle.getError().getMessage(), containsString("Error response!"));
  }

  @Test
  public void shouldFail_errorRemoteCall() throws ExecutionException, InterruptedException {
    // Given:
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeRemote));
    final PushRouting routing = new PushRouting();
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenReturn(createErrorFuture(new RuntimeException("Error remote!")));

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    PushConnectionsHandle handle = future.get();

    // Then:
    assertThat(handle.getError().getMessage(), containsString("Error remote!"));
  }

  @Test
  public void shouldFail_hitRequestLimitLocal() throws ExecutionException, InterruptedException {
    // Given:
    transientQueryQueue = new TransientQueryQueue(OptionalInt.empty(), 1, 100);
    when(pushRoutingOptions.getIsSkipForwardRequest()).thenReturn(true);
    final PushRouting routing = new PushRouting();
    BufferedPublisher<List<?>> localPublisher = new BufferedPublisher<>(context);
    when(pushPhysicalPlan.execute()).thenReturn(localPublisher);

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    PushConnectionsHandle handle = future.get();
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
    });

    // Then:
    List<List<?>> rows = new ArrayList<>();
    while (rows.size() < 1) {
      final KeyValue<List<?>, GenericRow> kv = transientQueryQueue.poll();
      if (kv == null) {
        Thread.sleep(100);
        continue;
      }
      rows.add(kv.value().values());
    }
    assertThat(rows.get(0), is(LOCAL_ROW1));
    assertThat(handle.getError().getMessage(), containsString("Hit limit of request queue"));
  }

  @Test
  public void shouldFail_hitRequestLimitRemote() throws ExecutionException, InterruptedException {
    // Given:
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeRemote));
    transientQueryQueue = new TransientQueryQueue(OptionalInt.empty(), 1, 100);
    final PushRouting routing = new PushRouting();
    BufferedPublisher<StreamedRow> remotePublisher = new BufferedPublisher<>(context);
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenReturn(createFuture(RestResponse.successful(200, remotePublisher)));

    // When:
    CompletableFuture<PushConnectionsHandle> future =
        routing.handlePushQuery(serviceContext, pushPhysicalPlan, statement, pushRoutingOptions,
            outputSchema, transientQueryQueue);
    PushConnectionsHandle handle = future.get();
    context.runOnContext(v -> {
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
    });

    // Then:
    List<List<?>> rows = new ArrayList<>();
    while (rows.size() < 1) {
      final KeyValue<List<?>, GenericRow> kv = transientQueryQueue.poll();
      if (kv == null) {
        Thread.sleep(100);
        continue;
      }
      rows.add(kv.value().values());
    }
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(handle.getError().getMessage(), containsString("Hit limit of request queue"));
  }
}
