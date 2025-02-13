/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.integration;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.Pair;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HighAvailabilityTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityTestUtil.class);
  private static final Pattern QUERY_ID_PATTERN = Pattern.compile("query with ID (\\S+)");

  static ClusterStatusResponse sendClusterStatusRequest(
      final TestKsqlRestApp restApp) {
    return sendClusterStatusRequest(restApp, Optional.empty());
  }

  static ClusterStatusResponse sendClusterStatusRequest(
      final TestKsqlRestApp restApp,
      final Optional<BasicCredentials> credentials) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(credentials)) {
      final RestResponse<ClusterStatusResponse> res = restClient
          .makeClusterStatusRequest();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }
      return res.getResponse();
    }
  }

  static void sendHeartbeartsForWindowLength(
      final TestKsqlRestApp receiverApp,
      final KsqlHostInfoEntity sender,
      final long window
  ) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < window) {
      sendHeartbeatRequest(receiverApp, sender, System.currentTimeMillis());
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static ClusterStatusResponse  waitForRemoteServerToChangeStatus(
      final TestKsqlRestApp restApp,
      final KsqlHostInfoEntity remoteServer,
      final BiFunction<KsqlHostInfoEntity, Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean> function
  ) {
    return waitForRemoteServerToChangeStatus(restApp, remoteServer, function, Optional.empty());
  }

  static ClusterStatusResponse  waitForRemoteServerToChangeStatus(
      final TestKsqlRestApp restApp,
      final KsqlHostInfoEntity remoteServer,
      final BiFunction<KsqlHostInfoEntity, Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean> function,
      final Optional<BasicCredentials> credentials
  ) {
    return waitForRemoteServerToChangeStatus(
        restApp,
        response -> function.apply(remoteServer, response),
        credentials
    );
  }

  static ClusterStatusResponse  waitForRemoteServerToChangeStatus(
      final TestKsqlRestApp restApp,
      final Function<Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean> function,
      final Optional<BasicCredentials> credentials
  ) {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(restApp,
          credentials);
      if (function.apply(clusterStatusResponse.getClusterStatus())) {
        return clusterStatusResponse;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static void waitForClusterToBeDiscovered(
      final TestKsqlRestApp restApp,
      final int numServers
  ) {
    waitForClusterToBeDiscovered(restApp, numServers, Optional.empty());
  }

  static void waitForClusterToBeDiscovered(
      final TestKsqlRestApp restApp,
      final int numServers,
      final Optional<BasicCredentials> credentials
  ) {
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(
          restApp, credentials);
      if(allServersDiscovered(numServers, clusterStatusResponse.getClusterStatus())) {
        break;
      }
      try {
        Thread.sleep(200);
      } catch (final Exception e) {
        // Meh
      }
    }
  }

  static void waitForStreamsMetadataToInitialize(
      final TestKsqlRestApp restApp, List<KsqlHostInfoEntity> hosts
  ) {
    waitForStreamsMetadataToInitialize(restApp, hosts, Optional.empty());
  }

  static void waitForStreamsMetadataToInitialize(
      final TestKsqlRestApp restApp, List<KsqlHostInfoEntity> hosts,
      final Optional<BasicCredentials> credentials
  ) {
    while (true) {
      ClusterStatusResponse clusterStatusResponse
          = HighAvailabilityTestUtil.sendClusterStatusRequest(restApp, credentials);
      List<KsqlHostInfoEntity> initialized = hosts.stream()
          .filter(hostInfo -> Optional.ofNullable(
              clusterStatusResponse
                  .getClusterStatus()
                  .get(hostInfo))
              .map(hostStatusEntity -> hostStatusEntity
                  .getActiveStandbyPerQuery()
                  .isEmpty()).isPresent())
            .collect(Collectors.toList());
      if(initialized.size() == hosts.size())
        break;
    }
    try {
      Thread.sleep(200);
    } catch (final Exception e) {
      // Meh
    }
  }

  static boolean remoteServerIsDown(
      final KsqlHostInfoEntity remoteServer,
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus
  ) {
    if (!clusterStatus.containsKey(remoteServer)) {
      return true;
    }
    for( Entry<KsqlHostInfoEntity, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().getPort() == remoteServer.getPort()
          && !entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
  }

  static boolean remoteServerIsUp(
      final KsqlHostInfoEntity remoteServer,
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus
  ) {
    return clusterStatus.get(remoteServer).getHostAlive();
  }

  private static boolean allServersDiscovered(
      final int numServers,
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus
  ) {
    return clusterStatus.size() >= numServers;
  }

  private static void sendHeartbeatRequest(
      final TestKsqlRestApp restApp,
      final KsqlHostInfoEntity hostInfoEntity,
      final long timestamp
  ) {

    try (final KsqlRestClient restClient = restApp.buildInternalKsqlClient()) {
      restClient.makeAsyncHeartbeatRequest(hostInfoEntity, timestamp)
          .exceptionally(t -> {
            LOG.error("Unexpected exception in async request", t);
            return null;
          });
    }
  }

  public static HeartbeatResponse sendHeartbeatRequest(
      final TestKsqlRestApp restApp,
      final KsqlHostInfoEntity hostInfoEntity,
      final long timestamp,
      final Optional<BasicCredentials> userCreds
  ) {

    try (final KsqlRestClient restClient = restApp.buildInternalKsqlClient(userCreds)) {
      RestResponse<HeartbeatResponse> res = restClient.makeAsyncHeartbeatRequest(
          hostInfoEntity, timestamp)
          .exceptionally(t -> {
            LOG.error("Unexpected exception in async request", t);
            return null;
          }).get();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }
      return res.getResponse();
    } catch (ExecutionException | InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  public static HeartbeatResponse sendHeartbeatRequestNormalListener(
      final TestKsqlRestApp restApp,
      final KsqlHostInfoEntity hostInfoEntity,
      final long timestamp,
      final Optional<BasicCredentials> userCreds
  ) {

    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {
      RestResponse<HeartbeatResponse> res = restClient.makeAsyncHeartbeatRequest(
          hostInfoEntity, timestamp)
          .exceptionally(t -> {
            LOG.error("Unexpected exception in async request", t);
            return null;
          }).get();

      if (res.isErroneous()) {
        throw new AssertionError("Erroneous result: " + res.getErrorMessage());
      }
      return res.getResponse();
    } catch (ExecutionException | InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  public static void sendLagReportingRequest(
      final TestKsqlRestApp restApp,
      final LagReportingMessage lagReportingMessage
  ) throws ExecutionException, InterruptedException {

    try (final KsqlRestClient restClient = restApp.buildInternalKsqlClient()) {
      restClient.makeAsyncLagReportingRequest(lagReportingMessage)
          .exceptionally(t -> {
            LOG.error("Unexpected exception in async request", t);
            return null;
          })
          .get();
    }
  }

  static String extractQueryId(final String outputString) {
    final java.util.regex.Matcher matcher = QUERY_ID_PATTERN.matcher(outputString);
    if (!matcher.find()) {
      throw new AssertionError("Could not find query id in: " + outputString);
    }
    return matcher.group(1);
  }

  // Ensures that lags have been reported for the cluster.  Makes the simplified assumption that
  // there's just one state store.
  static BiFunction<KsqlHostInfoEntity, Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean>
  lagsReported(
      final int expectedClusterSize
  ) {
    return (remoteServer, clusterStatus) -> {
      if (clusterStatus.size() == expectedClusterSize) {
        int numWithLag = 0;
        for (Entry<KsqlHostInfoEntity, HostStatusEntity> e : clusterStatus.entrySet()) {
          if (e.getValue().getHostStoreLags().getStateStoreLags().size() > 0) {
            numWithLag++;
          }
        }
        if (numWithLag >= Math.min(expectedClusterSize, 2)) {
          LOG.info("Found expected lags: {}", clusterStatus.toString());
          return true;
        }
      }
      LOG.info("Didn't yet find expected lags: {}", clusterStatus.toString());
      return false;
    };
  }

  // Ensures that lags have been reported for the given host.  Makes the simplified assumption that
  // there's just one state store.
  static BiFunction<KsqlHostInfoEntity, Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean>
  lagsReported(
      final KsqlHostInfoEntity server,
      final Optional<Long> currentOffset,
      final long endOffset
  ) {
    return (remote, clusterStatus) -> {
      HostStatusEntity hostStatusEntity = clusterStatus.get(server);
      if (hostStatusEntity == null) {
        LOG.info("Didn't find {}", server.toString());
        return false;
      }
      Pair<Long, Long> pair = getOffsets(server,clusterStatus);
      long current = pair.left;
      long end = pair.right;
      if ((!currentOffset.isPresent() || current >= currentOffset.get()) && end >= endOffset) {
        LOG.info("Found expected current offset {} end offset {} for {}: {}", current, endOffset,
            server, clusterStatus.toString());
        return true;
      }
      LOG.info("Didn't yet find expected end offset {} for {}: {}", endOffset, server,
          clusterStatus.toString());
      return false;
    };
  }

  // Ensures that lags have been reported for the given host, but that the value is zero.
  // Makes the simplified assumption that there's just one state store.
  static BiFunction<KsqlHostInfoEntity, Map<KsqlHostInfoEntity, HostStatusEntity>, Boolean>
  zeroLagsReported(
      final KsqlHostInfoEntity server
  ) {
    return (remote, clusterStatus) -> {
      HostStatusEntity hostStatusEntity = clusterStatus.get(server);
      if (hostStatusEntity == null) {
        LOG.info("Didn't find {}", server.toString());
        return false;
      }
      Pair<Long, Long> pair = getLag(server,clusterStatus);
      long lag = pair.left;
      long end = pair.right;
      if (end > 0 && lag == 0) {
        LOG.info("Found zero lag for {}: {}", server, clusterStatus.toString());
        return true;
      }
      LOG.info("Didn't yet find > 0 end offset: {} and zero lag: {} for {}: {}", end, lag, server,
          clusterStatus.toString());
      return false;
    };
  }

  // Gets (current, end) offsets for the given host.  Makes the simplified assumption that there's
  // just one state store.
  public static Pair<Long, Long> getOffsets(
      final KsqlHostInfoEntity server,
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus) {
    HostStatusEntity hostStatusEntity = clusterStatus.get(server);
    long end = hostStatusEntity.getHostStoreLags().getStateStoreLags().values().stream()
        .flatMap(stateStoreLags -> stateStoreLags.getLagByPartition().values().stream())
        .mapToLong(LagInfoEntity::getEndOffsetPosition)
        .max()
        .orElse(0);
    long current = hostStatusEntity.getHostStoreLags().getStateStoreLags().values().stream()
        .flatMap(stateStoreLags -> stateStoreLags.getLagByPartition().values().stream())
        .mapToLong(LagInfoEntity::getCurrentOffsetPosition)
        .max()
        .orElse(0);
    return Pair.of(current, end);
  }

  // Gets (lag, end) for the given host.  Makes the simplified assumption that there's
  // just one state store.
  public static Pair<Long, Long> getLag(
      final KsqlHostInfoEntity server,
      final Map<KsqlHostInfoEntity, HostStatusEntity> clusterStatus) {
    HostStatusEntity hostStatusEntity = clusterStatus.get(server);
    long end = hostStatusEntity.getHostStoreLags().getStateStoreLags().values().stream()
        .flatMap(stateStoreLags -> stateStoreLags.getLagByPartition().values().stream())
        .mapToLong(LagInfoEntity::getEndOffsetPosition)
        .max()
        .orElse(0);
    long lag = hostStatusEntity.getHostStoreLags().getStateStoreLags().values().stream()
        .flatMap(stateStoreLags -> stateStoreLags.getLagByPartition().values().stream())
        .mapToLong(LagInfoEntity::getOffsetLag)
        .max()
        .orElse(-1);
    return Pair.of(lag, end);
  }

  // A class that holds shutoff switches for various network components in our system to simulate
  // slowdowns or partitions.
  public static class Shutoffs {
    private final AtomicBoolean ksqlOutgoing = new AtomicBoolean(false);
    private final AtomicInteger kafkaPauseOffset = new AtomicInteger(-1);

    public void shutOffAll() {
      ksqlOutgoing.set(true);
      kafkaPauseOffset.set(0);
    }

    public void reset() {
      ksqlOutgoing.set(false);
      kafkaPauseOffset.set(-1);
    }

    public void setKsqlOutgoing(boolean ksqlOutgoingPaused) {
      ksqlOutgoing.set(ksqlOutgoingPaused);
    }

    public void setKafkaPauseOffset(int pauseOffset) {
      kafkaPauseOffset.set(pauseOffset);
    }

    public Boolean getKsqlOutgoing() {
      return ksqlOutgoing.get();
    }

    public Integer getKafkaPauseOffset() {
      return kafkaPauseOffset.get();
    }

  }

  public static void makeAdminRequest(TestKsqlRestApp restApp, final String sql) {
    RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  public static void makeAdminRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds
  ) {
    RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, userCreds);
  }

  public static List<KsqlEntity> makeAdminRequestWithResponse(
      TestKsqlRestApp restApp, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, Optional.empty());
  }

  public static List<KsqlEntity> makeAdminRequestWithResponse(
      TestKsqlRestApp restApp, final String sql, final Optional<BasicCredentials> userCreds) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql, userCreds);
  }

  public static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql
  ) {
    return makePullQueryRequest(target, sql, null);
  }

  public static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, Optional.empty(),
        properties, ImmutableMap.of(KsqlRequestConfig.KSQL_DEBUG_REQUEST, true));
  }

  public static List<StreamedRow> makePullQueryRequest(
      final TestKsqlRestApp target,
      final String sql,
      final Map<String, ?> properties,
      final Optional<BasicCredentials> userCreds
  ) {
    return RestIntegrationTestUtil.makeQueryRequest(target, sql, userCreds,
        properties, ImmutableMap.of(KsqlRequestConfig.KSQL_DEBUG_REQUEST, true));
  }

  public static List<String> makePullQueryWsRequest(
      final URI uri,
      final String sql,
      final String mediaType,
      final String contentType,
      final Credentials credentials,
      final Optional<Map<String, Object>> overrides,
      final Optional<Map<String, Object>> requestProperties
  ) {
    return RestIntegrationTestUtil.makeWsRequest(
        uri,
        sql,
        Optional.of(mediaType),
        Optional.of(contentType),
        Optional.of(credentials),
        Optional.empty(),
        Optional.empty()
    );
  }
}

