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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class BinPackedStreamsMetadata implements QueryMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(BinPackedStreamsMetadata.class);

  private final KafkaStreamsBuilder kafkaStreamsBuilder;
  private final QueryErrorClassifier errorClassifier ;
  private final KafkaStreams kafkaStreams;
  private final ImmutableMap<String, Object> streamsProperties;
  private final QueryMetadataImpl.TimeBoundedQueue queryErrors;
  private final List<QueryMetadata.Listener> listeners;


  BinPackedStreamsMetadata(final KafkaStreamsBuilder kafkaStreamsBuilder,
                           final QueryErrorClassifier errorClassifier,
                           final int maxQueryErrorsQueueSize,
                           final Map<String, Object> streamsProperties) {
    this.kafkaStreamsBuilder = kafkaStreamsBuilder;
    this.errorClassifier = errorClassifier;
    this.kafkaStreams = kafkaStreamsBuilder.build(null, streamsProperties);
    this.streamsProperties =
        ImmutableMap.copyOf(
            Objects.requireNonNull(streamsProperties, "streamsProperties"));
    this.queryErrors
      = new QueryMetadataImpl.TimeBoundedQueue(Duration.ofHours(1), maxQueryErrorsQueueSize);
    this.listeners = new ArrayList<>();
  }

  protected StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      final Throwable e
  ) {
    QueryError.Type errorType = QueryError.Type.UNKNOWN;
    try {
      errorType = errorClassifier.classify(e);
    } catch (final Exception classificationException) {
      LOG.error("Error classifying unhandled exception", classificationException);
    } finally {
      // If error classification throws then we consider the error to be an UNKNOWN error.
      // We notify listeners and add the error to the errors queue in the finally block to ensure
      // all listeners and consumers of the error queue (e.g. the API) can see the error. Similarly,
      // log in finally block to make sure that if there's ever an error in the classification
      // we still get this in our logs.
      final QueryError queryError =
          new QueryError(
              System.currentTimeMillis(),
              Throwables.getStackTraceAsString(e),
              errorType
          );
      for (QueryMetadata.Listener listener: listeners) {
        listener.onError(, queryError);
      }
      queryErrors.add(queryError);
      LOG.error(
          "Unhandled exception caught in streams thread {}. ({})",
          Thread.currentThread().getName(),
          errorType,
          e
      );
    }
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
  }

  public void addListener(QueryMetadata.Listener listener) {

  }

  public KafkaStreams getKafkaStreams() {
    return null;
  }

  public KafkaStreams.State state() {
    return null;
  }

  public Collection<StreamsMetadata> allMetadata() {
    return null;
  }

  public Set<StreamsTaskMetadata> getTaskMetadata() {
    return null;
  }

  public void addToplogy(final Topology topology) {
  }

  public void stop(final QueryId queryId) {

  }

  public void restart(final QueryId queryId) {

  }

  public boolean isError(final QueryId queryId) {

  }

  public void close(final QueryId queryId) {

  }

  public void start(final QueryId queryId) {

  }

  public List<QueryError> getQueryErrors(final QueryId queryId) {
    return queryErrors.toImmutableList();
  }

  public Map<String, Map<Integer, LagInfo>> allLocalStorePartitionLags(final QueryId queryId) {
  }

  public Map<String, Object> getStreamProperties() {
    return streamsProperties;
  }

  public List<QueryError> getQueryErrors() {
    return queryErrors.toImmutableList();
  }
}
