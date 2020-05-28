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

import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HighAvailabilityTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityTestUtil.class);

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
    while (true) {
      final ClusterStatusResponse clusterStatusResponse = sendClusterStatusRequest(restApp,
          credentials);
      if (function.apply(remoteServer, clusterStatusResponse.getClusterStatus())) {
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
      final TestKsqlRestApp restApp, List<KsqlHostInfoEntity> hosts, String queryId
  ) {
    while (true) {
      ClusterStatusResponse clusterStatusResponse = HighAvailabilityTestUtil.sendClusterStatusRequest(restApp);
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
    for( Entry<KsqlHostInfoEntity, HostStatusEntity> entry: clusterStatus.entrySet()) {
      if (entry.getKey().getPort() == remoteServer.getPort()
          && entry.getValue().getHostAlive()) {
        return true;
      }
    }
    return false;
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
}

