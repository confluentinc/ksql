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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.util.QueryMetadataImpl.TimeBoundedQueue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SharedKafkaStreamsRuntime {

  private final Logger log = LoggerFactory.getLogger(SharedKafkaStreamsRuntime.class);

  protected final KafkaStreamsBuilder kafkaStreamsBuilder;
  protected KafkaStreamsNamedTopologyWrapper kafkaStreams;
  protected ImmutableMap<String, Object> streamsProperties;
  protected final Map<QueryId, BinPackedPersistentQueryMetadataImpl> collocatedQueries;

  protected SharedKafkaStreamsRuntime(
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final Map<String, Object> streamsProperties) {
    this.kafkaStreamsBuilder = kafkaStreamsBuilder;
    this.kafkaStreams = kafkaStreamsBuilder.buildNamedTopologyWrapper(streamsProperties);
    this.streamsProperties = ImmutableMap.copyOf(streamsProperties);
    this.collocatedQueries = new ConcurrentHashMap<>();
  }

  public abstract void register(
      BinPackedPersistentQueryMetadataImpl binpackedPersistentQueryMetadata
  );

  public boolean isError(final QueryId queryId) {
    return !collocatedQueries.get(queryId).getQueryErrors().isEmpty();
  }

  public abstract TimeBoundedQueue getNewQueryErrorQueue();

  public abstract void close();

  public abstract void stop(QueryId queryId, boolean isCreateOrReplace);

  public abstract void start(QueryId queryId);

  public abstract void restartStreamsRuntime();

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "streams must be exposed")
  public KafkaStreamsNamedTopologyWrapper getKafkaStreams() {
    return kafkaStreams;
  }

  public KafkaStreams.State state() {
    return kafkaStreams.state();
  }

  public Collection<StreamsMetadata> getAllStreamsClientsMetadataForQuery(final QueryId queryId) {
    return kafkaStreams.allStreamsClientsMetadataForTopology(queryId.toString());
  }

  public Set<StreamsTaskMetadata> getAllTaskMetadataForQuery(final QueryId queryId) {
    return kafkaStreams.metadataForLocalThreads()
        .stream()
        .flatMap(t -> t.activeTasks().stream())
        .filter(m -> queryId.toString().equals(m.taskId().topologyName()))
        .map(StreamsTaskMetadata::fromStreamsTaskMetadata)
        .collect(Collectors.toSet());
  }

  public Map<String, Map<Integer, LagInfo>> getAllLocalStorePartitionLagsForQuery(
      final QueryId queryId) {
    try {
      return kafkaStreams.allLocalStorePartitionLagsForTopology(queryId.toString());
    } catch (IllegalStateException | StreamsException e) {
      log.error(e.getMessage());
      return ImmutableMap.of();
    }
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "streamsProperties is immutable")
  public Map<String, Object> getStreamProperties() {
    return streamsProperties;
  }

  public Set<SourceName> getSources() {
    return collocatedQueries.values()
        .stream()
        .flatMap(t -> t.getSourceNames().stream())
        .collect(Collectors.toSet());
  }

  public Set<QueryId> getQueries() {
    return ImmutableSet.copyOf(collocatedQueries.keySet());
  }

  public KafkaStreamsBuilder getKafkaStreamsBuilder() {
    return kafkaStreamsBuilder;
  }

  public Map<QueryId, BinPackedPersistentQueryMetadataImpl> getCollocatedQueries() {
    return ImmutableMap.copyOf(collocatedQueries);
  }

  public String getApplicationId() {
    return getStreamProperties().get(StreamsConfig.APPLICATION_ID_CONFIG).toString();
  }

  public abstract void overrideStreamsProperties(Map<String, Object> newStreamsProperties);
}
