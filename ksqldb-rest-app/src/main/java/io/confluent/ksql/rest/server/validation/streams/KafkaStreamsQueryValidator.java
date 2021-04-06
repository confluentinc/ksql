/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.validation.streams;

import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;

public final class KafkaStreamsQueryValidator implements QueryEventListener {
  private final KsqlConfig config;
  private final boolean isSandbox;
  private final ConcurrentMap<QueryId, QueryProperties> perQueryInfo;

  public KafkaStreamsQueryValidator(final KsqlConfig config) {
    this(config, false, new ConcurrentHashMap<>());
  }

  private KafkaStreamsQueryValidator(
      final KsqlConfig config,
      final boolean isSandbox,
      final ConcurrentMap<QueryId, QueryProperties> perQueryInfo
  ) {
    this.config = Objects.requireNonNull(config, "config");
    this.isSandbox = isSandbox;
    this.perQueryInfo =
        new ConcurrentHashMap<>(Objects.requireNonNull(perQueryInfo, "perQueryInfo"));
  }

  private void validate(final QueryProperties queryInfo) {
    if (queryInfo.isPersistent()) {
      validateCacheBytesUsage(
          getPersistentQueryInfo(),
          queryInfo,
          config.getLong(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING)
      );
      validateStateStoreUsage(
          getPersistentQueryInfo(),
          queryInfo,
          config.getInt(KsqlConfig.KSQL_STATE_STORE_MAX)
      );
    } else {
      validateCacheBytesUsage(
          getTransientQueryInfo(),
          queryInfo,
          config.getLong(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT)
      );
    }
  }

  @Override
  public void onCreate(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata
  ) {
    final QueryProperties queryInfo = info(serviceContext, metaStore, queryMetadata);
    if (queryMetadata instanceof TransientQueryMetadata || isSandbox) {
      // Only validate persistent queries against the sandbox
      validate(queryInfo);
    }
    perQueryInfo.put(queryMetadata.getQueryId(), queryInfo);
  }

  @Override
  public void onDeregister(final QueryMetadata queryMetadata) {
    perQueryInfo.remove(queryMetadata.getQueryId());
  }

  @Override
  public Optional<QueryEventListener> createSandbox() {
    return Optional.of(new KafkaStreamsQueryValidator(config, true, perQueryInfo));
  }

  private Collection<QueryProperties> getPersistentQueryInfo() {
    return perQueryInfo.values().stream()
        .filter(QueryProperties::isPersistent)
        .collect(Collectors.toList());
  }

  private Collection<QueryProperties> getTransientQueryInfo() {
    return perQueryInfo.values().stream()
        .filter(QueryProperties::isTransient)
        .collect(Collectors.toList());
  }

  private void validateStateStoreUsage(
      final Collection<QueryProperties> queries,
      final QueryProperties queryInfo,
      final int limit
  ) {
    if (limit < 0) {
      return;
    }
    final int usedByRunning = queries.stream()
        .mapToInt(QueryProperties::getNumStores)
        .sum();
    if (queryInfo.getNumStores() + usedByRunning > limit) {
      throw new KsqlException(String.format(
          "Query would use %d stores, which would put the number of state store instances over "
              + "the configured limit (%d). Current number of stores is %d.",
          queryInfo.getNumStores(),
          limit,
          usedByRunning
      ));
    }
  }

  private void validateCacheBytesUsage(
      final Collection<QueryProperties> queries,
      final QueryProperties queryInfo,
      final long limit
  ) {
    if (limit < 0) {
      return;
    }
    final long usedByRunning = queries.stream()
        .mapToLong(QueryProperties::getCacheBytesUsage)
        .sum();
    if (queryInfo.getCacheBytesUsage() + usedByRunning > limit) {
      throw new KsqlException(String.format(
          "Configured cache usage (cache.max.bytes.buffering=%d) would put usage over the "
              + "configured limit (%d). Current usage is %d",
          queryInfo.getCacheBytesUsage(), usedByRunning, limit
      ));
    }
  }

  private int estimateNumStoreInstancesForQuery(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata query
  ) {
    final TopologyDescription topologyDescription = query.getTopology().describe();
    final Set<String> stores = topologyDescription.subtopologies().stream()
        .flatMap(s -> s.nodes().stream())
        .filter(n -> n instanceof Processor)
        .flatMap(n -> ((Processor) n).stores().stream())
        .collect(Collectors.toSet());
    final int partitions = query.getSourceNames().stream()
        .mapToInt(n -> partitionsForSource(serviceContext, metaStore, n))
        .max()
        .orElseThrow(() -> new IllegalStateException("query must have at least one source"));
    return partitions * stores.size();
  }

  private int partitionsForSource(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final SourceName name
  ) {
    final KafkaTopicClient c = serviceContext.getTopicClient();
    final TopicDescription desc = c.describeTopic(metaStore.getSource(name).getKafkaTopicName());
    return desc.partitions().size();
  }

  private QueryProperties info(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final QueryMetadata queryMetadata
  ) {
    return new QueryProperties(
        estimateNumStoreInstancesForQuery(serviceContext, metaStore, queryMetadata),
        new StreamsConfig(queryMetadata.getStreamsProperties())
            .getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG),
        queryMetadata instanceof PersistentQueryMetadata
    );
  }

  private static final class QueryProperties {
    final int numStores;
    final long cacheBytesUsage;
    final boolean persistent;

    private QueryProperties(
        final int numStores,
        final long cacheBytesUsage,
        final boolean persistent
    ) {
      this.numStores = numStores;
      this.cacheBytesUsage = cacheBytesUsage;
      this.persistent = persistent;
    }

    public int getNumStores() {
      return numStores;
    }

    public long getCacheBytesUsage() {
      return cacheBytesUsage;
    }

    public boolean isPersistent() {
      return persistent;
    }

    public boolean isTransient() {
      return !persistent;
    }
  }
}
