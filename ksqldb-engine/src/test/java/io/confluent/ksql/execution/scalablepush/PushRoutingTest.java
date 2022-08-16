package io.confluent.ksql.execution.scalablepush;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.internal.ScalablePushQueryMetrics;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.execution.common.OffsetsRow;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.execution.scalablepush.PushRouting.PushConnectionsHandle;
import io.confluent.ksql.execution.scalablepush.PushRouting.RoutingResult;
import io.confluent.ksql.execution.scalablepush.PushRouting.RoutingResultStatus;
import io.confluent.ksql.execution.scalablepush.locator.PushLocator;
import io.confluent.ksql.execution.scalablepush.locator.PushLocator.KsqlNode;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.PushOffsetRange;
import io.confluent.ksql.util.PushOffsetVector;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PushRoutingTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder().build();
  private static final QueryRow LOCAL_ROW1 = QueryRowImpl.of(SCHEMA, GenericKey.genericKey(),
      Optional.empty(), GenericRow.fromList(ImmutableList.of(1, "a")), 0);
  private static final QueryRow LOCAL_ROW2 = QueryRowImpl.of(SCHEMA, GenericKey.genericKey(),
      Optional.empty(), GenericRow.fromList(ImmutableList.of(2, "b")), 0);

  private static final QueryRow LOCAL_CONTINUATION_TOKEN1
      = OffsetsRow.of(0, new PushOffsetRange(
          Optional.of(new PushOffsetVector(ImmutableList.of(0L, 1L))),
          new PushOffsetVector(ImmutableList.of(0L, 3L))));
  private static final QueryRow LOCAL_CONTINUATION_TOKEN_GAP
      = OffsetsRow.of(0, new PushOffsetRange(
      Optional.of(new PushOffsetVector(ImmutableList.of(0L, 5L))),
      new PushOffsetVector(ImmutableList.of(0L, 7L))));

  private static final StreamedRow REMOTE_ROW1
      = StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of(3, "c")));
  private static final StreamedRow REMOTE_ROW2
      = StreamedRow.pushRow(GenericRow.fromList(ImmutableList.of(4, "d")));
  private static final StreamedRow REMOTE_CONTINUATION_TOKEN1
      = StreamedRow.continuationToken(new PushContinuationToken(
          new PushOffsetRange(
              Optional.of(new PushOffsetVector(ImmutableList.of(0L, 1L))),
              new PushOffsetVector(ImmutableList.of(0L, 3L))).serialize()));
  private static final StreamedRow REMOTE_CONTINUATION_TOKEN_GAP
      = StreamedRow.continuationToken(new PushContinuationToken(
      new PushOffsetRange(
          Optional.of(new PushOffsetVector(ImmutableList.of(0L, 5L))),
          new PushOffsetVector(ImmutableList.of(0L, 7L))).serialize()));

  private static final String CONSUMER_GROUP = "consumer_group";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SimpleKsqlClient simpleKsqlClient;
  @Mock
  private PushPhysicalPlanManager pushPhysicalPlanManager;
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
  private KsqlNode ksqlNodeRemote2;
  @Mock
  private TransientQueryQueue transientQueryQueueMock;
  @Mock
  private Optional<ScalablePushQueryMetrics> scalablePushQueryMetrics;

  private Vertx vertx;
  private Context context;
  private TransientQueryQueue transientQueryQueue;
  private TestLocalPublisher localPublisher;
  private TestRemotePublisher remotePublisher;

  @Rule
  public final Timeout timeout = Timeout.builder()
      .withTimeout(10, TimeUnit.SECONDS)
      .withLookingForStuckThread(true)
      .build();

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    context = vertx.getOrCreateContext();
    when(statement.getMaskedStatementText()).thenReturn("SELECT * FROM STREAM EMIT CHANGES");
    when(statement.getUnMaskedStatementText()).thenReturn("SELECT * FROM STREAM EMIT CHANGES");
    when(statement.getSessionConfig()).thenReturn(sessionConfig);
    when(sessionConfig.getOverrides()).thenReturn(ImmutableMap.of());
    when(serviceContext.getKsqlClient()).thenReturn(simpleKsqlClient);
    when(pushPhysicalPlanManager.getScalablePushRegistry()).thenReturn(scalablePushRegistry);
    when(pushPhysicalPlanManager.getContext()).thenReturn(context);
    when(pushPhysicalPlanManager.closeable()).thenReturn(() -> {});
    when(pushPhysicalPlanManager.getCatchupConsumerGroupId()).thenReturn(CONSUMER_GROUP);
    when(scalablePushRegistry.getLocator()).thenReturn(locator);
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeLocal, ksqlNodeRemote));
    when(ksqlNodeLocal.location()).thenReturn(URI.create("http://localhost:8088"));
    when(ksqlNodeLocal.isLocal()).thenReturn(true);
    when(ksqlNodeRemote.location()).thenReturn(URI.create("http://remote:8088"));
    when(ksqlNodeRemote.isLocal()).thenReturn(false);
    when(ksqlNodeRemote2.location()).thenReturn(URI.create("http://remote2:8088"));
    when(ksqlNodeRemote2.isLocal()).thenReturn(false);
    when(pushRoutingOptions.getHasBeenForwarded()).thenReturn(false);
    when(pushRoutingOptions.alosEnabled()).thenReturn(true);
    localPublisher = new TestLocalPublisher(context);
    remotePublisher = new TestRemotePublisher(context);
    when(pushPhysicalPlanManager.execute()).thenReturn(localPublisher);
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenReturn(createFuture(RestResponse.successful(200, remotePublisher)));

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

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
    });

    // Then:
    Set<List<?>> rows = waitOnRows(4);
    handle.close();
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
  }

  @Test
  public void shouldSucceed_addRemoteNode() throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
    });

    Set<List<?>> rows = waitOnRows(2);

    nodes.set(ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    context.runOnContext(v -> {
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
    });

    // Then:
    rows.addAll(waitOnRows(2));
    handle.close();
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
  }

  @Test
  public void shouldSucceed_removeRemoteNode() throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
    });

    Set<List<?>> rows = waitOnRows(4);

    final RoutingResult result = handle.get(ksqlNodeRemote).get();
    nodes.set(ImmutableSet.of(ksqlNodeLocal));
    while (handle.get(ksqlNodeRemote).isPresent()) {
      Thread.sleep(100);
      continue;
    }
    handle.close();

    // Then:
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
    assertThat(result.getStatus(), is(RoutingResultStatus.REMOVED));
  }

  @Test
  public void shouldSucceed_remoteNodeComplete() throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, false);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
      remotePublisher.complete();
    });

    Set<List<?>> rows = waitOnRows(4);
    waitOnNodeStatus(handle, ksqlNodeRemote, RoutingResultStatus.COMPLETE);
    handle.close();

    // Then:
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
    assertThat(handle.get(ksqlNodeRemote).get().getStatus(), is(RoutingResultStatus.COMPLETE));
  }

  @Test
  public void shouldSucceed_remoteNodeExceptionWithRetry() throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);
    AtomicReference<TestRemotePublisher> remotePublisher = new AtomicReference<>();
    AtomicInteger remoteCount = new AtomicInteger(0);
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenAnswer(a -> {
          remotePublisher.set(new TestRemotePublisher(context));
          if (remoteCount.incrementAndGet() == 2) {
            remotePublisher.get().accept(REMOTE_ROW1);
            remotePublisher.get().accept(REMOTE_ROW2);
          }
          return createFuture(RestResponse.successful(200, remotePublisher.get()));
        });

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    final AtomicReference<Throwable> exception = new AtomicReference<>(null);
    handle.onException(exception::set);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
      remotePublisher.get().error(new RuntimeException("Random error"));
    });

    Set<List<?>> rows = waitOnRows(4);
    handle.close();

    // Then:
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
  }

  @Test
  public void shouldFail_LocaleNodeException() throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
      localPublisher.error(new RuntimeException("Random local error"));
    });

    final Throwable throwable = waitOnException(handle);
    handle.close();

    // Then:
    assertThat(throwable.getMessage(), containsString("Random local error"));
  }

  @Test
  public void shouldSucceed_justForwarded() throws ExecutionException, InterruptedException {
    // Given:
    when(pushRoutingOptions.getHasBeenForwarded()).thenReturn(true);
    final PushRouting routing = new PushRouting();

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
    });

    // Then:
    verify(simpleKsqlClient, never()).makeQueryRequestStreamed(any(), any(), any(), any());
    Set<List<?>> rows = waitOnRows(2);
    handle.close();
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
  }

  @Test
  public void shouldFail_duringPlanExecute() throws ExecutionException, InterruptedException {
    // Given:
    when(pushRoutingOptions.getHasBeenForwarded()).thenReturn(true);
    final PushRouting routing = new PushRouting();
    when(pushPhysicalPlanManager.execute()).thenThrow(new RuntimeException("Error!"));

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);

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
    final PushConnectionsHandle handle = handlePushRouting(routing);

    // Then:
    assertThat(handle.getError().getMessage(), containsString("Error response!"));
  }

  @Test
  public void shouldFail_errorRemoteCall() throws ExecutionException, InterruptedException {
    // Given:
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeRemote, ksqlNodeRemote2));
    final PushRouting routing = new PushRouting();
    TestRemotePublisher remotePublisher = new TestRemotePublisher(context);
    when(simpleKsqlClient.makeQueryRequestStreamed(
        eq(ksqlNodeRemote.location()), any(), any(), any()))
        .thenReturn(createErrorFuture(new RuntimeException("Error remote!")));
    when(simpleKsqlClient.makeQueryRequestStreamed(
        eq(ksqlNodeRemote2.location()), any(), any(), any()))
        .thenReturn(createFuture(RestResponse.successful(200, remotePublisher)));

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);

    // Then:
    assertThat(handle.getError().getMessage(), containsString("Error remote!"));
    assertThat(remotePublisher.isClosed(), is(true));
  }

  @Test
  public void shouldFail_hitRequestLimitLocal() throws ExecutionException, InterruptedException {
    // Given:
    transientQueryQueue = new TransientQueryQueue(OptionalInt.empty(), 1, 100);
    when(pushRoutingOptions.getHasBeenForwarded()).thenReturn(true);
    final PushRouting routing = new PushRouting();
    BufferedPublisher<QueryRow> localPublisher = new BufferedPublisher<>(context);
    when(pushPhysicalPlanManager.execute()).thenReturn(localPublisher);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_ROW2);
    });

    // Then:
    Set<List<?>> rows = waitOnRows(1);
    handle.close();
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(handle.getError().getMessage(), containsString("Hit limit of request queue"));
  }

  @Test
  public void shouldFail_hitRequestLimitRemote() throws ExecutionException, InterruptedException {
    // Given:
    when(locator.locate()).thenReturn(ImmutableList.of(ksqlNodeRemote));
    transientQueryQueue = new TransientQueryQueue(OptionalInt.empty(), 1, 100);
    final PushRouting routing = new PushRouting();

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_ROW2);
    });

    // Then:
    Set<List<?>> rows = waitOnRows(1);
    handle.close();
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(handle.getError().getMessage(), containsString("Hit limit of request queue"));
  }

  @Test
  public void shouldSucceed_gapDetectedRemote_noRetry()
      throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, false);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      remotePublisher.accept(REMOTE_ROW1);
      remotePublisher.accept(REMOTE_CONTINUATION_TOKEN1);
      remotePublisher.accept(REMOTE_ROW2);
      remotePublisher.accept(REMOTE_CONTINUATION_TOKEN_GAP);
    });

    Set<List<?>> rows = waitOnRows(2);
    waitOnNodeStatus(handle, ksqlNodeRemote, RoutingResultStatus.OFFSET_GAP_FOUND);
    handle.close();

    // Then:
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
    assertThat(handle.get(ksqlNodeRemote).get().getStatus(),
        is(RoutingResultStatus.OFFSET_GAP_FOUND));
  }

  @Test
  public void shouldSucceed_gapDetectedRemote_retry()
      throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);
    AtomicReference<TestRemotePublisher> remotePublisher = new AtomicReference<>();
    AtomicInteger remoteCount = new AtomicInteger(0);
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenAnswer(a -> {
          remotePublisher.set(new TestRemotePublisher(context));
          remoteCount.incrementAndGet();
          final Map<String, ?> requestProperties = a.getArgument(3);
          String continuationToken = (String) requestProperties.get(
              KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN);
          if (remoteCount.get() == 1) {
            assertThat(continuationToken, nullValue());
          } else if (remoteCount.get()  == 2) {
            assertThat(continuationToken, notNullValue());
            final PushOffsetRange range = PushOffsetRange.deserialize(continuationToken);
            assertThat(range.getEndOffsets().getDenseRepresentation(),
                is(ImmutableList.of(0L, 3L)));
            remotePublisher.get().accept(REMOTE_ROW2);
          }
          return createFuture(RestResponse.successful(200, remotePublisher.get()));
        });

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    final AtomicReference<Throwable> exception = new AtomicReference<>(null);
    handle.onException(exception::set);
    context.runOnContext(v -> {
      remotePublisher.get().accept(REMOTE_CONTINUATION_TOKEN1);
      remotePublisher.get().accept(REMOTE_ROW1);
      remotePublisher.get().accept(REMOTE_CONTINUATION_TOKEN_GAP);
    });

    Set<List<?>> rows = waitOnRows(2);
    handle.close();

    // Then:
    verify(simpleKsqlClient, times(2)).makeQueryRequestStreamed(any(), any(), any(), any());
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
  }

  @Test
  public void shouldSucceed_gapDetectedRemote_disableAlos()
      throws ExecutionException, InterruptedException {
    // Given:
    when(pushRoutingOptions.alosEnabled()).thenReturn(false);
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);
    AtomicReference<TestRemotePublisher> remotePublisher = new AtomicReference<>();
    AtomicInteger remoteCount = new AtomicInteger(0);
    when(simpleKsqlClient.makeQueryRequestStreamed(any(), any(), any(), any()))
        .thenAnswer(a -> {
          remotePublisher.set(new TestRemotePublisher(context));
          remoteCount.incrementAndGet();
          final Map<String, ?> requestProperties = a.getArgument(3);
          String continuationToken = (String) requestProperties.get(
              KsqlRequestConfig.KSQL_REQUEST_QUERY_PUSH_CONTINUATION_TOKEN);
          if (remoteCount.get() == 1) {
            assertThat(continuationToken, nullValue());
            context.runOnContext(v -> {
              remotePublisher.get().accept(REMOTE_ROW2);
            });
          } else if (remoteCount.get()  == 2) {
            fail();
          }
          return createFuture(RestResponse.successful(200, remotePublisher.get()));
        });

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    final AtomicReference<Throwable> exception = new AtomicReference<>(null);
    handle.onException(exception::set);
    context.runOnContext(v -> {
      remotePublisher.get().accept(REMOTE_CONTINUATION_TOKEN1);
      remotePublisher.get().accept(REMOTE_ROW1);
      remotePublisher.get().accept(REMOTE_CONTINUATION_TOKEN_GAP);
    });

    Set<List<?>> rows = waitOnRows(2);
    handle.close();

    // Then:
    verify(simpleKsqlClient, times(1)).makeQueryRequestStreamed(any(), any(), any(), any());
    assertThat(rows.contains(REMOTE_ROW1.getRow().get().getColumns()), is(true));
    assertThat(rows.contains(REMOTE_ROW2.getRow().get().getColumns()), is(true));
  }

  @Test
  public void shouldSucceed_gapDetectedLocal_noRetry()
      throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, false);

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.accept(LOCAL_ROW1);
      localPublisher.accept(LOCAL_CONTINUATION_TOKEN1);
      localPublisher.accept(LOCAL_ROW2);
      localPublisher.accept(LOCAL_CONTINUATION_TOKEN_GAP);
    });

    Set<List<?>> rows = waitOnRows(2);
    waitOnNodeStatus(handle, ksqlNodeLocal, RoutingResultStatus.OFFSET_GAP_FOUND);
    handle.close();

    // Then:
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
    assertThat(handle.get(ksqlNodeLocal).get().getStatus(),
        is(RoutingResultStatus.OFFSET_GAP_FOUND));
  }

  @Test
  public void shouldSucceed_gapDetectedLocal_retry()
      throws ExecutionException, InterruptedException {
    // Given:
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);
    AtomicReference<TestLocalPublisher> localPublisher = new AtomicReference<>();
    AtomicInteger localCount = new AtomicInteger(0);
    when(pushPhysicalPlanManager.execute()).thenAnswer(a -> {
      localPublisher.set(new TestLocalPublisher(context));
      localCount.incrementAndGet();
      if (localCount.get() == 2) {
        localPublisher.get().accept(LOCAL_ROW2);
      }
      return localPublisher.get();
    });
    doAnswer(a -> {
      final Optional<PushOffsetRange> newOffsetRange = a.getArgument(0);
      assertThat(newOffsetRange.isPresent(), is(true));
      assertThat(newOffsetRange.get().getEndOffsets(), is(ImmutableList.of(0L, 3L)));
      return null;
    }).when(pushPhysicalPlanManager).reset(any());

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.get().accept(LOCAL_CONTINUATION_TOKEN1);
      localPublisher.get().accept(LOCAL_ROW1);
      localPublisher.get().accept(LOCAL_CONTINUATION_TOKEN_GAP);
    });

    Set<List<?>> rows = waitOnRows(2);
    handle.close();

    // Then:
    verify(pushPhysicalPlanManager, times(2)).execute();
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
  }

  @Test
  public void shouldSucceed_gapDetectedLocal_disableAlos()
      throws ExecutionException, InterruptedException {
    // Given:
    when(pushRoutingOptions.alosEnabled()).thenReturn(false);
    final AtomicReference<Set<KsqlNode>> nodes = new AtomicReference<>(
        ImmutableSet.of(ksqlNodeLocal, ksqlNodeRemote));
    final PushRouting routing = new PushRouting(sqr -> nodes.get(), 50, true);
    AtomicReference<TestLocalPublisher> localPublisher = new AtomicReference<>();
    AtomicInteger localCount = new AtomicInteger(0);
    when(pushPhysicalPlanManager.execute()).thenAnswer(a -> {
      localPublisher.set(new TestLocalPublisher(context));
      localCount.incrementAndGet();
      context.runOnContext(v -> {
        localPublisher.get().accept(LOCAL_ROW2);
      });
      return localPublisher.get();
    });
    doAnswer(a -> {
      final Optional<PushOffsetRange> newOffsetRange = a.getArgument(0);
      assertThat(newOffsetRange.isPresent(), is(true));
      assertThat(newOffsetRange.get().getEndOffsets(), is(ImmutableList.of(0L, 3L)));
      return null;
    }).when(pushPhysicalPlanManager).reset(any());

    // When:
    final PushConnectionsHandle handle = handlePushRouting(routing);
    context.runOnContext(v -> {
      localPublisher.get().accept(LOCAL_CONTINUATION_TOKEN1);
      localPublisher.get().accept(LOCAL_ROW1);
      localPublisher.get().accept(LOCAL_CONTINUATION_TOKEN_GAP);
    });

    Set<List<?>> rows = waitOnRows(2);
    handle.close();

    // Then:
    verify(pushPhysicalPlanManager, times(1)).execute();
    assertThat(rows.contains(LOCAL_ROW1.value().values()), is(true));
    assertThat(rows.contains(LOCAL_ROW2.value().values()), is(true));
  }

  private Set<List<?>> waitOnRows(final int numRows) throws InterruptedException {
    Set<List<?>> rows = new HashSet<>();
    while (rows.size() < numRows) {
      final KeyValueMetadata<List<?>, GenericRow> kv = transientQueryQueue.poll();
      if (kv == null) {
        Thread.sleep(10);
        continue;
      }
      rows.add(kv.getKeyValue().value().values());
    }
    return rows;
  }

  private void waitOnNodeStatus(
      final PushConnectionsHandle handle,
      final KsqlNode ksqlNode,
      final RoutingResultStatus status
  ) throws InterruptedException {
    while (!handle.get(ksqlNode).isPresent()
        || handle.get(ksqlNode).get().getStatus() != status) {
      Thread.sleep(10);
      continue;
    }
  }

  private Throwable waitOnException(
      final PushConnectionsHandle handle
  ) throws InterruptedException {
    final AtomicReference<Throwable> exception = new AtomicReference<>(null);
    handle.onException(exception::set);
    while (exception.get() == null) {
      Thread.sleep(10);
      continue;
    }
    return exception.get();
  }

  private PushConnectionsHandle handlePushRouting(final PushRouting routing)
      throws ExecutionException, InterruptedException {
    return routing.handlePushQuery(serviceContext, pushPhysicalPlanManager, statement,
        pushRoutingOptions, outputSchema, transientQueryQueue, scalablePushQueryMetrics,
        Optional.empty())
        .get();
  }

  private static class TestRemotePublisher extends BufferedPublisher<StreamedRow> {

    private volatile boolean closed = false;

    public TestRemotePublisher(Context ctx) {
      super(ctx);
    }

    public void error(final Throwable e) {
      sendError(e);
    }

    public Future<Void> close() {
      closed = true;
      return super.close();
    }

    public boolean isClosed() {
      return closed;
    }
  }

  private static class TestLocalPublisher extends BufferedPublisher<QueryRow> {

    private volatile boolean closed = false;

    public TestLocalPublisher(Context ctx) {
      super(ctx);
    }

    public void error(final Throwable e) {
      sendError(e);
    }

    public Future<Void> close() {
      closed = true;
      return super.close();
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
