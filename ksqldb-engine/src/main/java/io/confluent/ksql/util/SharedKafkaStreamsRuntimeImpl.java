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
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedKafkaStreamsRuntimeImpl extends SharedKafkaStreamsRuntime {

  private final Logger log = LoggerFactory.getLogger(SharedKafkaStreamsRuntimeImpl.class);

  public SharedKafkaStreamsRuntimeImpl(final KafkaStreamsBuilder kafkaStreamsBuilder,
                                       final int maxQueryErrorsQueueSize,
                                       final QueryErrorClassifier errorClassifier,
                                       final Map<String, Object> streamsProperties) {
    super(kafkaStreamsBuilder, maxQueryErrorsQueueSize, errorClassifier, streamsProperties);
    kafkaStreams.start();
  }

  public void register(final SharedRuntimePersistentQueryMetadata persistentQueryMetadata) {
    super.register(persistentQueryMetadata);
    log.info("Registered query {} in runtime {}",
             persistentQueryMetadata.getQueryId(), getApplicationId());
  }

  // CHECKSTYLE_RULES.OFF: TodoComment
  public void stop(final QueryId queryId) {
    log.info("Attempting to stop Query: " + queryId.toString());
    if (queriesInSharedRuntime.containsKey(queryId) && sources.containsKey(queryId)) {
      if (kafkaStreams.state().isRunningOrRebalancing()) {
        kafkaStreams.removeNamedTopology(queryId.toString());
      } else {
        throw new IllegalStateException("Streams in not running but is in state"
            + kafkaStreams.state());
      }

      queriesInSharedRuntime.remove(queryId);
      // TODO KCI-470: Once remove is blocking this can be uncommented for now it breaks
      // kafkaStreams.cleanUpNamedTopology(queryId.toString());
      // sources.remove(queryId);
    }
  }

  public synchronized void close(final boolean cleanup) {
    kafkaStreams.close();
    if (cleanup) {
      kafkaStreams.cleanUp();
    }
  }

  public void start(final QueryId queryId) {
    if (queriesInSharedRuntime.containsKey(queryId)
        && !queriesInSharedRuntime.get(queryId).everStarted) {
      if (!kafkaStreams.getTopologyByName(queryId.toString()).isPresent()) {
        kafkaStreams.addNamedTopology(queriesInSharedRuntime.get(queryId).getTopology());
      } else {
        throw new IllegalArgumentException("not done removing query: " + queryId);
      }
    } else {
      throw new IllegalArgumentException("query: " + queryId + " not added to runtime");
    }
  }

}