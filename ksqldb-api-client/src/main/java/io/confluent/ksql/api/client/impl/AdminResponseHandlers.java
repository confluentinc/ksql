/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.client.impl;

import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

final class AdminResponseHandlers {

  private AdminResponseHandlers() {
  }

  static void handleListStreamsResponse(
      final JsonObject streamsListEntity,
      final CompletableFuture<List<StreamInfo>> cf
  ) {
    try {
      final JsonArray streams = streamsListEntity.getJsonArray("streams");
      cf.complete(streams.stream()
          .map(o -> (JsonObject) o)
          .map(o -> new StreamInfoImpl(
              o.getString("name"),
              o.getString("topic"),
              o.getString("format")))
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + streamsListEntity));
    }
  }

  static void handleListTablesResponse(
      final JsonObject tablesListEntity,
      final CompletableFuture<List<TableInfo>> cf
  ) {
    try {
      final JsonArray tables = tablesListEntity.getJsonArray("tables");
      cf.complete(tables.stream()
          .map(o -> (JsonObject) o)
          .map(o -> new TableInfoImpl(
              o.getString("name"),
              o.getString("topic"),
              o.getString("format"),
              o.getBoolean("isWindowed")))
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + tablesListEntity));
    }
  }

  static void handleListTopicsResponse(
      final JsonObject kafkaTopicsListEntity,
      final CompletableFuture<List<TopicInfo>> cf
  ) {
    try {
      final JsonArray topics = kafkaTopicsListEntity.getJsonArray("topics");
      cf.complete(topics.stream()
          .map(o -> (JsonObject) o)
          .map(o -> {
            final List<Integer> replicaInfo = o.getJsonArray("replicaInfo").stream()
                .map(v -> (Integer)v)
                .collect(Collectors.toList());
            return new TopicInfoImpl(
                o.getString("name"),
                replicaInfo.size(),
                replicaInfo);
          })
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + kafkaTopicsListEntity));
    }
  }

  static void handleListQueriesResponse(
      final JsonObject queriesEntity,
      final CompletableFuture<List<QueryInfo>> cf
  ) {
    try {
      final JsonArray queries = queriesEntity.getJsonArray("queries");
      cf.complete(queries.stream()
          .map(o -> (JsonObject) o)
          .map(o -> {
            final QueryType queryType = QueryType.valueOf(o.getString("queryType"));
            final JsonArray sinks = o.getJsonArray("sinks");
            final JsonArray sinkTopics = o.getJsonArray("sinkKafkaTopics");

            final Optional<String> sinkName;
            final Optional<String> sinkTopicName;
            if (queryType == QueryType.PERSISTENT) {
              if (sinks.size() != 1 || sinkTopics.size() != 1) {
                throw new IllegalStateException("Persistent queries must have exactly one sink.");
              }
              sinkName = Optional.of(sinks.getString(0));
              sinkTopicName = Optional.of(sinkTopics.getString(0));
            } else if (queryType == QueryType.PUSH) {
              if (sinks.size() != 0 || sinkTopics.size() != 0) {
                throw new IllegalStateException("Push queries must have no sinks.");
              }
              sinkName = Optional.empty();
              sinkTopicName = Optional.empty();
            } else {
              throw new IllegalStateException("Unexpected query type.");
            }

            return new QueryInfoImpl(
                queryType,
                o.getString("id"),
                o.getString("queryString"),
                sinkName,
                sinkTopicName);
          })
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + queriesEntity));
    }
  }

}
