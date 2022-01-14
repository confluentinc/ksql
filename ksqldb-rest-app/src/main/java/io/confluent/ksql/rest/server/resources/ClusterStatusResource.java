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

package io.confluent.ksql.rest.server.resources;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ActiveStandbyEntity;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.HostStatusEntity;
import io.confluent.ksql.rest.entity.HostStoreLags;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.TopicPartitionEntity;
import io.confluent.ksql.rest.server.HeartbeatAgent;
import io.confluent.ksql.rest.server.LagReportingAgent;
import io.confluent.ksql.util.HostStatus;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Endpoint that reports the view of the cluster that this server has.
 * Returns every host that has been discovered by this server along side with information about its
 * status such as whether it is alive or dead and the last time its status got updated.
 */

public class ClusterStatusResource {

  private final KsqlEngine engine;
  private final HeartbeatAgent heartbeatAgent;
  private final Optional<LagReportingAgent> lagReportingAgent;
  static final HostStoreLags EMPTY_HOST_STORE_LAGS =
      new HostStoreLags(ImmutableMap.of(), 0);

  public ClusterStatusResource(
      final KsqlEngine engine,
      final HeartbeatAgent heartbeatAgent,
      final Optional<LagReportingAgent> lagReportingAgent) {
    this.engine = requireNonNull(engine, "engine");
    this.heartbeatAgent = requireNonNull(heartbeatAgent, "heartbeatAgent");
    this.lagReportingAgent = requireNonNull(lagReportingAgent, "lagReportingAgent");
  }

  public EndpointResponse checkClusterStatus() {
    final ClusterStatusResponse response = getResponse();
    return EndpointResponse.ok(response);
  }

  private ClusterStatusResponse getResponse() {
    final Map<KsqlHostInfo, HostStatus> allHostStatus = heartbeatAgent.getHostsStatus();

    final Map<KsqlHostInfoEntity, HostStatusEntity> response = allHostStatus
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> new KsqlHostInfoEntity(entry.getKey().host(), entry.getKey().port()),
            entry -> new HostStatusEntity(entry.getValue().isHostAlive(),
                                          entry.getValue().getLastStatusUpdateMs(),
                                          getActiveStandbyInformation(entry.getKey()),
                                          getHostStoreLags(entry.getKey(),
                                              entry.getValue().isHostAlive()))
        ));

    return new ClusterStatusResponse(response);
  }

  private HostStoreLags getHostStoreLags(final KsqlHostInfo ksqlHostInfo, final boolean isAlive) {
    // The lag reporting agent currently caches lag info without regard to the current alive status,
    // so don't show lags for hosts we consider down.
    if (!isAlive) {
      return EMPTY_HOST_STORE_LAGS;
    }
    return lagReportingAgent
        .flatMap(agent -> agent.getLagPerHost(ksqlHostInfo))
        .orElse(EMPTY_HOST_STORE_LAGS);
  }

  private Map<String, ActiveStandbyEntity> getActiveStandbyInformation(
      final KsqlHostInfo ksqlHostInfo
  ) {
    final Map<String, ActiveStandbyEntity> perQueryMap = new HashMap<>();
    for (PersistentQueryMetadata persistentQueryMetadata: engine.getPersistentQueries()) {
      for (StreamsMetadata streamsMetadata: persistentQueryMetadata.getAllStreamsHostMetadata()) {
        if (!streamsMetadata.hostInfo().equals(asHostInfo(ksqlHostInfo))) {
          continue;
        }
        final QueryIdAndStreamsMetadata queryIdAndStreamsMetadata = new QueryIdAndStreamsMetadata(
            persistentQueryMetadata.getQueryId().toString(),
            streamsMetadata
        );
        perQueryMap.putIfAbsent(
            queryIdAndStreamsMetadata.queryId,
            queryIdAndStreamsMetadata.toActiveStandbyEntity()
        );
      }
    }
    return perQueryMap;
  }

  private static final class QueryIdAndStreamsMetadata {

    final String queryId;
    final StreamsMetadata streamsMetadata;

    QueryIdAndStreamsMetadata(
        final String queryId,
        final StreamsMetadata streamsMetadata
    ) {
      this.queryId = requireNonNull(queryId, "queryId");
      this.streamsMetadata = requireNonNull(streamsMetadata, "md");
    }

    public ActiveStandbyEntity toActiveStandbyEntity() {
      final Set<TopicPartitionEntity> activePartitions = streamsMetadata.topicPartitions()
          .stream()
          .map(topicPartition -> new TopicPartitionEntity(
              topicPartition.topic(), topicPartition.partition()))
          .collect(Collectors.toSet());

      final Set<TopicPartitionEntity> standByPartitions = streamsMetadata.standbyTopicPartitions()
          .stream()
          .map(topicPartition -> new TopicPartitionEntity(
              topicPartition.topic(), topicPartition.partition()))
          .collect(Collectors.toSet());

      return new ActiveStandbyEntity(
          streamsMetadata.stateStoreNames(),
          activePartitions,
          streamsMetadata.standbyStateStoreNames(),
          standByPartitions);
    }
  }

  private HostInfo asHostInfo(final KsqlHostInfo ksqlHostInfo) {
    return new HostInfo(ksqlHostInfo.host(), ksqlHostInfo.port());
  }
}
