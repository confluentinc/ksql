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

import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SandboxedSharedKafkaStreamsRuntimeImpl extends SharedKafkaStreamsRuntime {
  private final Logger log = LoggerFactory.getLogger(SandboxedSharedKafkaStreamsRuntimeImpl.class);

  public SandboxedSharedKafkaStreamsRuntimeImpl(
      final SharedKafkaStreamsRuntime sharedRuntime
  ) {
    super(
        sharedRuntime.getKafkaStreamsBuilder(),
        getSandboxStreamsProperties(sharedRuntime),
        new QueryMetadataImpl.TimeBoundedQueue(Duration.ofHours(1), 0)
    );

    for (BinPackedPersistentQueryMetadataImpl query : sharedRuntime.collocatedQueries.values()) {
      kafkaStreams.addNamedTopology(query.getTopologyCopy());
    }
  }

  public SandboxedSharedKafkaStreamsRuntimeImpl(
      final KafkaStreamsBuilder kafkaStreamsBuilder,
      final int maxQueryErrorsQueueSize,
      final Map<String, Object> streamsProperties
  ) {
    super(
        kafkaStreamsBuilder,
        streamsProperties,
        new QueryMetadataImpl.TimeBoundedQueue(Duration.ofHours(1), maxQueryErrorsQueueSize)
    );
  }

  private static Map<String, Object> getSandboxStreamsProperties(
      final SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime
  ) {
    final Map<String, Object> sandboxStreamsProperties =
        new ConcurrentHashMap<>(sharedKafkaStreamsRuntime.getStreamProperties());
    sandboxStreamsProperties.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        sharedKafkaStreamsRuntime.getStreamProperties().get(StreamsConfig.APPLICATION_ID_CONFIG)
            + UUID.randomUUID().toString()
            + "-validation"
    );
    return sandboxStreamsProperties;
  }

  @Override
  public void register(
      final QueryErrorClassifier errorClassifier,
      final BinPackedPersistentQueryMetadataImpl binpackedPersistentQueryMetadata,
      final QueryId queryId
  ) {
    if (!sources.containsKey(queryId)) {
      if (sources
          .values()
          .stream()
          .flatMap(Collection::stream)
          .anyMatch(t -> binpackedPersistentQueryMetadata.getSourceNames().contains(t))) {
        throw new IllegalArgumentException(
            queryId.toString() + ": was not reserved on this runtime");
      } else {
        sources.put(queryId, binpackedPersistentQueryMetadata.getSourceNames());
      }
    }
    collocatedQueries.put(queryId, binpackedPersistentQueryMetadata);
    log.debug("mapping {}", collocatedQueries);
  }

  @Override
  public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable e
  ) {
    return StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  @Override
  public void stop(final QueryId queryId) {
  }

  @Override
  public synchronized void close() {
    log.debug("Closing validation runtime {}", getApplicationId());
    kafkaStreams.close();
    kafkaStreams.cleanUp();
  }

  @Override
  public void start(final QueryId queryId) {
  }

  @Override
  public void restartStreamsRuntime() {
  }
}
