package io.confluent.ksql.services;

import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FaultyKsqlClient implements SimpleKsqlClient {
  private static final Logger LOG = LoggerFactory.getLogger(FaultyKsqlClient.class);

  private final SimpleKsqlClient workingClient;
  private final Supplier<Boolean> isFaulty;

  public FaultyKsqlClient(
      final SimpleKsqlClient workingClient,
      final Supplier<Boolean> isFaulty
  ) {
    this.workingClient = workingClient;
    this.isFaulty = isFaulty;
  }

  private SimpleKsqlClient getClient() {
    if (isFaulty.get()) {
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
      final Map<String, ?> configOverrides, Map<String, ?> requestProperties) {
    return getClient().makeQueryRequest(serverEndPoint, sql, configOverrides, requestProperties);
  }

  @Override
  public void makeAsyncHeartbeatRequest(final URI serverEndPoint, KsqlHostInfo host,
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
}
