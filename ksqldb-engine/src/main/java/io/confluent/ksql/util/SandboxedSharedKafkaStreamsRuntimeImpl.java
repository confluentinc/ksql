/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.util;

import static io.confluent.ksql.query.QueryBuilder.buildQueryImplementation;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SandboxedSharedKafkaStreamsRuntimeImpl extends SharedKafkaStreamsRuntime {
  private final Logger log = LoggerFactory.getLogger(SharedKafkaStreamsRuntimeImpl.class);

  public SandboxedSharedKafkaStreamsRuntimeImpl(
      final SharedKafkaStreamsRuntime originalRuntime,
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final Map<String, Object> streamsProperties
  ) {
    super(kafkaStreamsBuilder,
         originalRuntime.errorClassifier,
         streamsProperties,
         new QueryMetadataImpl.TimeBoundedQueue(Duration.ZERO, 0),
         new ConcurrentHashMap<>(originalRuntime.getSourcesMap()),
         new ConcurrentHashMap<>()
    );
    // A new sandbox runtime is created for each query validation phase, so instead of starting
    // the runtime we must build it up again by adding the NamedTopologies of all other queries
    // that were in the runtime
    final Collection<SharedRuntimePersistentQueryMetadata> realQueriesInSharedRuntime
        = originalRuntime.getQueriesInSharedRuntime().values();
    for (SharedRuntimePersistentQueryMetadata queryMetadata : realQueriesInSharedRuntime) {
      final NamedTopologyBuilder topologyBuilder =
          kafkaStreams.newNamedTopologyBuilder(queryMetadata.getQueryId().toString());
      buildQueryImplementation(
          queryMetadata.getPhysicalPlan(),
          queryMetadata.getRuntimeBuildContext(topologyBuilder)
      );
      kafkaStreams.addNamedTopology(topologyBuilder.build());
      queriesInSharedRuntime.put(
          queryMetadata.getQueryId(),
          SandboxedSharedRuntimePersistentQueryMetadata.of(queryMetadata)
      );
    }
  }

  public void register(final SharedRuntimePersistentQueryMetadata persistentQueryMetadata) {
    super.register(persistentQueryMetadata);
    // Because start(query) is never called for a sandbox query during validation, we must add the
    // NamedTopology now although the sandbox runtime itself is never started
    kafkaStreams.addNamedTopology(persistentQueryMetadata.getTopology());
  }

  public Collection<StreamsMetadata> allMetadata() {
    return kafkaStreams.metadataForAllStreamsClients();
  }

  public Set<StreamsTaskMetadata> getTaskMetadata() {
    return kafkaStreams.metadataForLocalThreads()
        .stream()
        .flatMap(t -> t.activeTasks().stream())
        .map(StreamsTaskMetadata::fromStreamsTaskMetadata)
        .collect(Collectors.toSet());
  }

  public boolean isError(final QueryId queryId) {
    return false;
  }

  public void stop(final QueryId queryId) {
    // nothing to do
  }

  public synchronized void close(final boolean cleanup) {
    // nothing to do
  }

  public void start(final QueryId queryId) {
    // nothing to do
  }

  public List<QueryError> getRuntimeErrors() {
    return Collections.emptyList();
  }

  public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags(final QueryId queryId) {
    return ImmutableMap.of();
  }

  public void addRuntimeError(final QueryError e) {
  }
}
