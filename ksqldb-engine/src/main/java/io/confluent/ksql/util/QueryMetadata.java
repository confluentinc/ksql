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

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.LagInfo;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public interface QueryMetadata {
  void initialize();

  Set<StreamsTaskMetadata> getTaskMetadata();

  Map<String, Object> getOverriddenProperties();

  String getStatementString();

  void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler);

  KafkaStreams.State getState();

  String getExecutionPlan();

  String getQueryApplicationId();

  Topology getTopology();

  Map<String, Map<Integer, LagInfo>> getAllLocalStorePartitionLags();

  Collection<StreamsMetadata> getAllStreamsHostMetadata();

  Map<String, Object> getStreamsProperties();

  LogicalSchema getLogicalSchema();

  Set<SourceName> getSourceNames();

  boolean hasEverBeenStarted();

  QueryId getQueryId();

  KsqlConstants.KsqlQueryType getQueryType();

  String getTopologyDescription();

  List<QueryError> getQueryErrors();

  void setCorruptionQueryError();

  KafkaStreams getKafkaStreams();

  void close();

  void start();

  interface RetryEvent {

    long nextRestartTimeMs();

    int getNumRetries();

    void backOff();
  }

  interface Listener {
    /**
     * This method will be called whenever the underlying application
     * throws an uncaught exception.
     *
     * @param error the error that occurred
     */
    void onError(QueryMetadata queryMetadata, QueryError error);

    void onStateChange(
        QueryMetadata queryMetadata,
        KafkaStreams.State before,
        KafkaStreams.State after);

    void onClose(QueryMetadata queryMetadata);
  }
}
