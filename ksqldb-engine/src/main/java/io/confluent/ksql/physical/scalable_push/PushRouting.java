package io.confluent.ksql.physical.scalable_push;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.pull.HARouting;
import io.confluent.ksql.physical.pull.StandbyFallbackException;
import io.confluent.ksql.physical.scalable_push.locator.PushLocator.KsqlNode;
import io.confluent.ksql.query.TransientQueryQueue;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushRouting implements AutoCloseable {


  private static final Logger LOG = LoggerFactory.getLogger(HARouting.class);

//  private final ExecutorService executorService;

  public PushRouting(
      final KsqlConfig ksqlConfig
  ) {
//    this.executorService = Executors.newFixedThreadPool(
//        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG),
//        new ThreadFactoryBuilder().setNameFormat("pull-query-executor-%d").build());
  }

  @Override
  public void close() {
//    executorService.shutdown();
  }

  public CompletableFuture<PushConnectionsHandle> handlePushQuery(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final PushRoutingOptions pushRoutingOptions,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final WorkerExecutor workerExecutor
  ) {
    final Set<KsqlNode> hosts = pushPhysicalPlan.getScalablePushRegistry()
        .getLocator()
        .locate()
        .stream()
        .filter(node -> !pushRoutingOptions.getIsSkipForwardRequest() || node.isLocal())
        .collect(Collectors.toSet());

    if (hosts.isEmpty()) {
      LOG.debug("Unable to execute push query: {}. No nodes executing persistent queries",
          statement.getStatementText());
      throw new KsqlException(String.format(
          "Unable to execute push query. No nodes executing persistent queries %s",
          statement.getStatementText()));
    }

//    final CompletableFuture<PushConnectionsHandle> completableFuture = new CompletableFuture<>();
    final VertxCompletableFuture<PushConnectionsHandle> vcf = new VertxCompletableFuture<>();
    workerExecutor.executeBlocking(promise -> {
      try {
        PushConnectionsHandle pushConnectionsHandle =
            executeRounds(serviceContext, pushPhysicalPlan, statement, hosts, outputSchema,
                transientQueryQueue, workerExecutor);
        promise.complete(pushConnectionsHandle);
      } catch (Throwable t) {
        promise.fail(t);
      }
    }, false, vcf);

    return vcf;
  }

  private PushConnectionsHandle executeRounds(
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final ConfiguredStatement<Query> statement,
      final Set<KsqlNode> hosts,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final WorkerExecutor workerExecutor
  ) throws InterruptedException {
    final Map<KsqlNode, CompletableFuture<RoutingResult>> futures = new LinkedHashMap<>();
    for (final KsqlNode node : hosts) {
//      final CompletableFuture<RoutingResult> completableFuture = new CompletableFuture<>();
      final VertxCompletableFuture<RoutingResult> vcf = new VertxCompletableFuture<>();
      workerExecutor.executeBlocking(
          promise -> {
            try {
              final RoutingResult result = executeOrRouteQuery(
                  node, statement, serviceContext, pushPhysicalPlan, outputSchema,
                  transientQueryQueue, workerExecutor);
//              completableFuture.complete(result);
              promise.complete(result);
            } catch (Throwable t) {
//              completableFuture.completeExceptionally(t);
              promise.fail(t);
            }
          },
          false,
          vcf
      );
      futures.put(node, vcf);
    }

    final PushConnectionsHandle pushConnectionsHandle = new PushConnectionsHandle();
    for (Map.Entry<KsqlNode, CompletableFuture<RoutingResult>> entry : futures.entrySet()) {
      final Future<RoutingResult> future = entry.getValue();
      final KsqlNode node = entry.getKey();
      RoutingResult routingResult = null;
      try {
        routingResult = future.get();
      } catch (ExecutionException e) {
        LOG.warn("Error routing query {} to host {} at timestamp {} with exception {}",
            statement.getStatementText(), node, System.currentTimeMillis(), e.getCause());
        throw new MaterializationException(String.format(
            "Unable to execute pull query \"%s\". %s",
            statement.getStatementText(), e.getCause().getMessage()));
      }
      pushConnectionsHandle.add(node, routingResult);
    }
    return pushConnectionsHandle;
  }

  @VisibleForTesting
  static RoutingResult executeOrRouteQuery(
      final KsqlNode node,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final PushPhysicalPlan pushPhysicalPlan,
      final LogicalSchema outputSchema,
      final TransientQueryQueue transientQueryQueue,
      final WorkerExecutor workerExecutor
  ) {
    if (node.isLocal()) {
      try {
        LOG.debug("Query {} executed locally at host {} at timestamp {}.",
            statement.getStatementText(), node.location(), System.currentTimeMillis());
        BufferedPublisher<List<?>> publisher = pushPhysicalPlan.execute();
        publisher.subscribe(new LocalQueryStreamSubscriber(publisher.getContext(),
            transientQueryQueue, workerExecutor));
        return new RoutingResult(RoutingResultStatus.SUCCESS, pushPhysicalPlan::close);
      } catch (StandbyFallbackException e) {
        LOG.warn("Error executing query {} locally at node {}. Falling back to standby state which "
                + "may return stale results",
            statement.getStatementText(), node, e.getCause());
        return new RoutingResult(RoutingResultStatus.STANDBY_FALLBACK);
      } catch (Exception e) {
        LOG.error("Error executing query {} locally at node {}",
            statement.getStatementText(), node.location(), e.getCause());
        throw new KsqlException(
            String.format("Error executing query locally at node %s: %s", node.location(),
                e.getMessage()),
            e
        );
      }
    } else {
      try {
        LOG.debug("Query {} routed to host {} at timestamp {}.",
            statement.getStatementText(), node.location(), System.currentTimeMillis());
        final BufferedPublisher<StreamedRow> publisher = forwardTo(node, statement, serviceContext,
            transientQueryQueue, outputSchema, workerExecutor);
        return new RoutingResult(RoutingResultStatus.SUCCESS, publisher::close);
      } catch (StandbyFallbackException e) {
        LOG.warn("Error forwarding query {} to node {}. Falling back to standby state which may "
                + "return stale results",
            statement.getStatementText(), node.location(), e.getCause());
        return new RoutingResult(RoutingResultStatus.STANDBY_FALLBACK);
      } catch (Exception e) {
        LOG.error("Error forwarding query {} to node {}",
            statement.getStatementText(), node, e.getCause());
        throw new KsqlException(
            String.format("Error forwarding query to node %s: %s", node.location(),
                e.getMessage()),
            e
        );
      }
    }
  }

  private static BufferedPublisher<StreamedRow> forwardTo(
      final KsqlNode owner,
      final ConfiguredStatement<Query> statement,
      final ServiceContext serviceContext,
      final TransientQueryQueue transientQueryQueue,
      final LogicalSchema outputSchema,
      final WorkerExecutor workerExecutor
  ) {
    // Add skip forward flag to properties
    final Map<String, Object> requestProperties = ImmutableMap.of(
        KsqlRequestConfig.KSQL_REQUEST_QUERY_PULL_SKIP_FORWARDING, true,
        KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true);
    final RestResponse<BufferedPublisher<StreamedRow>> response;

    try {
      response = serviceContext
          .getKsqlClient()
          .makeQueryRequestStreamed(
              owner.location(),
              statement.getStatementText(),
              statement.getSessionConfig().getOverrides(),
              requestProperties
//              streamedRowsHandler(owner, transientQueryQueue, outputSchema)
          );
    } catch (Exception e) {
      // If we threw some explicit exception, then let it bubble up. All of the row handling is
      // wrapped in a KsqlException, so any intentional exception or bug will be surfaced.
      final KsqlException ksqlException = causedByKsqlException(e);
      if (ksqlException != null) {
        throw ksqlException;
      }
      // If we get some kind of unknown error, we assume it's network or other error from the
      // KsqlClient and try standbys
      throw new StandbyFallbackException(String.format(
          "Forwarding pull query request [%s, %s] failed with error %s ",
          statement.getSessionConfig().getOverrides(), requestProperties,
          e.getMessage()), e);
    }

    if (response.isErroneous()) {
      throw new KsqlException(String.format(
          "Forwarding pull query request [%s, %s] failed with error %s ",
          statement.getSessionConfig().getOverrides(), requestProperties,
          response.getErrorMessage()));
    }

    final BufferedPublisher<StreamedRow> publisher = response.getResponse();
    publisher.subscribe(new QueryStreamSubscriber(publisher.getContext(), transientQueryQueue,
        workerExecutor));
    return publisher;
  }

  private static KsqlException causedByKsqlException(final Exception e) {
    Throwable throwable = e;
    while (throwable != null) {
      if (throwable instanceof KsqlException) {
        return (KsqlException) throwable;
      }
      throwable = throwable.getCause();
    }
    return null;
  }

//  private static Consumer<List<StreamedRow>> streamedRowsHandler(
//      final KsqlNode owner,
//      final TransientQueryQueue transientQueryQueue,
//      final LogicalSchema outputSchema
//  ) {
//    final AtomicInteger processedRows = new AtomicInteger(0);
//    final AtomicReference<Header> header = new AtomicReference<>();
//    return streamedRows -> {
//      try {
//        if (streamedRows == null || streamedRows.isEmpty()) {
//          return;
//        }
//
//        // If this is the first row overall, skip the header
//        final int previousProcessedRows = processedRows.getAndAdd(streamedRows.size());
//        for (int i = 0; i < streamedRows.size(); i++) {
//          final StreamedRow row = streamedRows.get(i);
//          if (i == 0 && previousProcessedRows == 0) {
//            final Optional<Header> optionalHeader = row.getHeader();
//            optionalHeader.ifPresent(h -> validateSchema(outputSchema, h.getSchema(), owner));
//            optionalHeader.ifPresent(header::set);
//            continue;
//          }
//
//          if (row.getErrorMessage().isPresent()) {
//            // If we receive an error that's not a network error, we let that bubble up.
//            throw new KsqlException(row.getErrorMessage().get().getMessage());
//          }
//
//          if (!row.getRow().isPresent()) {
//            throw new KsqlException("Missing row data on row " + i + " of chunk");
//          }
//
//          final List<?> r = row.getRow().get().getColumns();
//          Preconditions.checkNotNull(header.get());
//
//          transientQueryQueue.acceptRow(null, GenericRow.fromList(r));
//        }
//      } catch (Exception e) {
//        throw new KsqlException("Error handling streamed rows: " + e.getMessage(), e);
//      }
//    };
//  }

//  private static void validateSchema(
//      final LogicalSchema expectedSchema,
//      final LogicalSchema forwardedSchema,
//      final KsqlNode forwardedNode
//  ) {
//    if (!forwardedSchema.equals(expectedSchema)) {
//      throw new KsqlException(String.format(
//          "Schemas %s from host %s differs from schema %s",
//          forwardedSchema, forwardedNode, expectedSchema));
//    }
//  }


  public enum RoutingResultStatus {
    SUCCESS,
    STANDBY_FALLBACK
  }

  public static class RoutingResult {
    private final RoutingResultStatus status;
    private final AutoCloseable closeable;

    public RoutingResult(final RoutingResultStatus status, final AutoCloseable closeable) {
      this.status = status;
      this.closeable = closeable;
    }

    public RoutingResult(final RoutingResultStatus status) {
      this.status = status;
      this.closeable = () -> {};
    }

    public void close() {
      try {
        closeable.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public RoutingResultStatus getStatus() {
      return status;
    }
  }

  private static class QueryStreamSubscriber extends BaseSubscriber<StreamedRow> {

    private final TransientQueryQueue transientQueryQueue;
    private final WorkerExecutor workerExecutor;
    private boolean closed;

    QueryStreamSubscriber(final Context context,
        final TransientQueryQueue transientQueryQueue,
        final WorkerExecutor workerExecutor) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.workerExecutor = workerExecutor;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final StreamedRow row) {
      if (closed) {
        return;
      }
      if (row.isTerminal()) {
        close();
        return;
      }
      if (row.getRow().isPresent()) {
        workerExecutor.executeBlocking(promise -> {
          transientQueryQueue.acceptRow(null, GenericRow.fromList(row.getRow().get().getColumns()));
          promise.complete();
        }, false, result -> {});
      }
      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
    }

    @Override
    protected void handleError(final Throwable t) {

    }

    synchronized void close() {
      closed = true;
      context.runOnContext(v -> cancel());
    }
  }

  private static class LocalQueryStreamSubscriber extends BaseSubscriber<List<?>> {

    private final TransientQueryQueue transientQueryQueue;
    private final WorkerExecutor workerExecutor;
    private boolean closed;

    LocalQueryStreamSubscriber(
        final Context context,
        final TransientQueryQueue transientQueryQueue,
        final WorkerExecutor workerExecutor
    ) {
      super(context);
      this.transientQueryQueue = transientQueryQueue;
      this.workerExecutor = workerExecutor;
    }

    @Override
    protected void afterSubscribe(final Subscription subscription) {
      makeRequest(1);
    }

    @Override
    protected synchronized void handleValue(final List<?> row) {
      if (closed) {
        return;
      }
      workerExecutor.executeBlocking(promise -> {
        transientQueryQueue.acceptRow(null, GenericRow.fromList(row));
        promise.complete();
      }, false, result -> {});

      makeRequest(1);
    }

    @Override
    protected void handleComplete() {
    }

    @Override
    protected void handleError(final Throwable t) {
    }

    synchronized void close() {
      closed = true;
      context.runOnContext(v -> cancel());
    }
  }

  public static class PushConnectionsHandle {
    final Map<KsqlNode, RoutingResult> results = new LinkedHashMap<>();

    public void add(final KsqlNode ksqlNode, RoutingResult result) {
      results.put(ksqlNode, result);
    }

    public void remove(final KsqlNode ksqlNode) {
      results.remove(ksqlNode);
    }

    public void close() {
      for (RoutingResult result : results.values()) {
        result.close();
      }
    }
  }
}
