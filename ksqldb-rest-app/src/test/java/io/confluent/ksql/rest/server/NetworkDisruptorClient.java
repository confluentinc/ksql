package io.confluent.ksql.rest.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlHostInfo;
import io.vertx.core.streams.WriteStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkDisruptorClient implements SimpleKsqlClient {

  private static final Logger LOG = LoggerFactory.getLogger(NetworkDisruptorClient.class);

  private final SimpleKsqlClient workingClient;
  private final NetworkState networkState;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public NetworkDisruptorClient(
      final SimpleKsqlClient workingClient,
      final NetworkState networkState
  ) {
    this.workingClient = workingClient;
    this.networkState = networkState;
  }

  private SimpleKsqlClient getClient() {
    if (networkState.isFaulty()) {
      throw new UnsupportedOperationException("KSQL client is disabled");
    }
    return workingClient;
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(final URI serverEndPoint, final String sql,
      final Map<String, ?> requestProperties) {
    return getClient().makeKsqlRequest(serverEndPoint, sql, requestProperties);
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties
  ) {
    return getClient().makeQueryRequest(serverEndPoint, sql, configOverrides, requestProperties);
  }

  @Override
  public RestResponse<Integer> makeQueryRequest(
      final URI serverEndPoint,
      final String sql,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties,
      final WriteStream<List<StreamedRow>> rowConsumer,
      final CompletableFuture<Void> shouldCloseConnection, final Function<StreamedRow, StreamedRow> addHostInfo) {
    return getClient().makeQueryRequest(serverEndPoint, sql, configOverrides, requestProperties,
        rowConsumer, shouldCloseConnection, Function.identity());
  }

  @Override
  public CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> makeQueryRequestStreamed(
      URI serverEndPoint, String sql, Map<String, ?> configOverrides,
      Map<String, ?> requestProperties) {
    if (networkState.isFaulty()) {
      CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> exception
          = new CompletableFuture<>();
      exception.completeExceptionally(new UnsupportedOperationException("KSQL client is disabled"));
      return exception;
    }
    final CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> result =
        getClient().makeQueryRequestStreamed(serverEndPoint, sql, configOverrides,
            requestProperties);
    networkState.addPublisher(result);
    return result;
  }

  @Override
  public void makeAsyncHeartbeatRequest(final URI serverEndPoint, final KsqlHostInfo host,
      final long timestamp) {
    getClient().makeAsyncHeartbeatRequest(serverEndPoint, host, timestamp);
  }

  @Override
  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest(final URI serverEndPoint) {
    return getClient().makeClusterStatusRequest(serverEndPoint);
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage) {
    getClient().makeAsyncLagReportRequest(serverEndPoint, lagReportingMessage);
  }

  @Override
  public void close() {
    try {
      workingClient.close();
    } catch (Throwable t) {
      LOG.error("Error closing client", t);
    }
  }

  public static class NetworkState {
    private final CopyOnWriteArrayList<CompletableFuture<RestResponse<
        BufferedPublisher<StreamedRow>>>> publishers = new CopyOnWriteArrayList<>();

    private final AtomicBoolean isFaulty = new AtomicBoolean(false);

    public NetworkState() {
    }

    public void clear() {
      isFaulty.set(false);
      publishers.clear();
    }

    public void addPublisher(
        final CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>> publisherFuture) {
      publishers.add(publisherFuture);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public List<CompletableFuture<RestResponse<BufferedPublisher<StreamedRow>>>> getPublishers() {
      return publishers;
    }

    public void setFaulty(boolean faulty) {
      isFaulty.set(faulty);
    }

    public boolean isFaulty() {
      return isFaulty.get();
    }
  }
}
