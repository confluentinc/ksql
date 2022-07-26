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
import io.confluent.ksql.api.client.ServerInfo;
import io.confluent.ksql.api.client.SourceDescription;
import io.confluent.ksql.api.client.StreamInfo;
import io.confluent.ksql.api.client.TableInfo;
import io.confluent.ksql.api.client.TopicInfo;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
final class AdminResponseHandlers {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private AdminResponseHandlers() {
  }

  static void handleListStreamsResponse(
      final JsonObject streamsListEntity,
      final CompletableFuture<List<StreamInfo>> cf
  ) {
    final Optional<List<StreamInfo>> streams = getListStreamsResponse(streamsListEntity);
    if (streams.isPresent()) {
      cf.complete(streams.get());
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + streamsListEntity));
    }
  }

  static void handleListTablesResponse(
      final JsonObject tablesListEntity,
      final CompletableFuture<List<TableInfo>> cf
  ) {
    final Optional<List<TableInfo>> tables = getListTablesResponse(tablesListEntity);
    if (tables.isPresent()) {
      cf.complete(tables.get());
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + tablesListEntity));
    }
  }

  static void handleListTopicsResponse(
      final JsonObject kafkaTopicsListEntity,
      final CompletableFuture<List<TopicInfo>> cf
  ) {
    final Optional<List<TopicInfo>> topics = getListTopicsResponse(kafkaTopicsListEntity);
    if (topics.isPresent()) {
      cf.complete(topics.get());
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + kafkaTopicsListEntity));
    }
  }

  static void handleListQueriesResponse(
      final JsonObject queriesEntity,
      final CompletableFuture<List<QueryInfo>> cf
  ) {
    final Optional<List<QueryInfo>> queries = getListQueriesResponse(queriesEntity);
    if (queries.isPresent()) {
      cf.complete(queries.get());
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + queriesEntity));
    }
  }

  static void handleDescribeSourceResponse(
      final JsonObject sourceDescriptionEntity,
      final CompletableFuture<SourceDescription> cf
  ) {
    final Optional<SourceDescription> source = getDescribeSourceResponse(sourceDescriptionEntity);
    if (source.isPresent()) {
      cf.complete(source.get());
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + sourceDescriptionEntity));
    }
  }

  static void handleServerInfoResponse(
      final JsonObject serverInfoEntity,
      final CompletableFuture<ServerInfo> cf
  ) {
    final JsonObject source = serverInfoEntity.getJsonObject("KsqlServerInfo");

    try {
      final ServerInfoImpl serverInfo = new ServerInfoImpl(
          source.getString("version"),
          source.getString("kafkaClusterId"),
          source.getString("ksqlServiceId")
      );
      cf.complete(serverInfo);
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + serverInfoEntity));
    }
  }

  static boolean isListStreamsResponse(final JsonObject ksqlEntity) {
    return getListStreamsResponse(ksqlEntity).isPresent();
  }

  static boolean isListTablesResponse(final JsonObject ksqlEntity) {
    return getListTablesResponse(ksqlEntity).isPresent();
  }

  static boolean isListTopicsResponse(final JsonObject ksqlEntity) {
    return getListTopicsResponse(ksqlEntity).isPresent();
  }

  static boolean isListQueriesResponse(final JsonObject ksqlEntity) {
    return getListQueriesResponse(ksqlEntity).isPresent();
  }

  static boolean isDescribeSourceResponse(final JsonObject ksqlEntity) {
    return getDescribeSourceResponse(ksqlEntity).isPresent();
  }

  static boolean isDescribeOrListFunctionResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getJsonArray("functions") != null;
  }

  static boolean isExplainQueryResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getJsonObject("queryDescription") != null;
  }

  static boolean isListPropertiesResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getJsonArray("properties") != null;
  }

  static boolean isListTypesResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getJsonObject("types") != null;
  }

  static boolean isListConnectorsResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getJsonArray("connectors") != null;
  }

  static boolean isDescribeConnectorResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getString("connectorClass") != null;
  }

  static boolean isCreateConnectorResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getJsonObject("info") != null
        || (ksqlEntity.getString("message") != null
        && ksqlEntity.getString("message").contains("already exists"));
  }

  static boolean isDropConnectorResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getString("connectorName") != null
        || (ksqlEntity.getString("message") != null
        && ksqlEntity.getString("message").contains("not exist"));
  }

  static boolean isConnectErrorResponse(final JsonObject ksqlEntity) {
    return ksqlEntity.getString("errorMessage") != null;
  }

  /**
   * Attempts to parse the provided response entity as a {@code StreamsListEntity}.
   *
   * @param streamsListEntity response entity
   * @return optional containing parsed result if successful, else empty
   */
  private static Optional<List<StreamInfo>> getListStreamsResponse(
      final JsonObject streamsListEntity
  ) {
    try {
      final JsonArray streams = streamsListEntity.getJsonArray("streams");
      return Optional.of(streams.stream()
          .map(o -> (JsonObject) o)
          .map(o -> new StreamInfoImpl(
              o.getString("name"),
              o.getString("topic"),
              o.getString("keyFormat", "KAFKA"),
              o.getString("valueFormat", o.getString("format", "UNKNOWN")),
              o.getBoolean("isWindowed", false)
          ))
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Attempts to parse the provided response entity as a {@code TablesListEntity}.
   *
   * @param tablesListEntity response entity
   * @return optional containing parsed result if successful, else empty
   */
  private static Optional<List<TableInfo>> getListTablesResponse(
      final JsonObject tablesListEntity
  ) {
    try {
      final JsonArray tables = tablesListEntity.getJsonArray("tables");
      return Optional.of(tables.stream()
          .map(o -> (JsonObject) o)
          .map(o -> new TableInfoImpl(
              o.getString("name"),
              o.getString("topic"),
              o.getString("keyFormat", "KAFKA"),
              o.getString("valueFormat", o.getString("format", "UNKNOWN")),
              o.getBoolean("isWindowed")))
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Attempts to parse the provided response entity as a {@code KafkaTopicsListEntity}.
   *
   * @param kafkaTopicsListEntity response entity
   * @return optional containing parsed result if successful, else empty
   */
  private static Optional<List<TopicInfo>> getListTopicsResponse(
      final JsonObject kafkaTopicsListEntity
  ) {
    try {
      final JsonArray topics = kafkaTopicsListEntity.getJsonArray("topics");
      return Optional.of(topics.stream()
          .map(o -> (JsonObject) o)
          .map(o -> {
            final List<Integer> replicaInfo = o.getJsonArray("replicaInfo").stream()
                .map(v -> (Integer) v)
                .collect(Collectors.toList());
            return new TopicInfoImpl(
                o.getString("name"),
                replicaInfo.size(),
                replicaInfo);
          })
          .collect(Collectors.toList())
      );
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Attempts to parse the provided response entity as a {@code QueriesEntity}.
   *
   * @param queriesEntity response entity
   * @return optional containing parsed result if successful, else empty
   */
  private static Optional<List<QueryInfo>> getListQueriesResponse(final JsonObject queriesEntity) {
    try {
      final JsonArray queries = queriesEntity.getJsonArray("queries");
      return Optional.of(formatQueries(queries));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private static List<QueryInfo> formatQueries(final JsonArray queries) {
    return queries.stream()
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
        .collect(Collectors.toList());
  }

  /**
   * Attempts to parse the provided response entity as a {@code SourceDescriptionEntity}.
   *
   * @param sourceDescriptionEntity response entity
   * @return optional containing parsed result if successful, else empty
   */
  private static Optional<SourceDescription> getDescribeSourceResponse(
      final JsonObject sourceDescriptionEntity
  ) {
    try {
      final JsonObject source = sourceDescriptionEntity.getJsonObject("sourceDescription");
      return Optional.of(new SourceDescriptionImpl(
          source.getString("name"),
          source.getString("type"),
          source.getJsonArray("fields").stream()
              .map(o -> (JsonObject)o)
              .map(f -> new FieldInfoImpl(
                  f.getString("name"),
                  new ColumnTypeImpl(f.getJsonObject("schema").getString("type")),
                  "KEY".equals(f.getString("type"))))
              .collect(Collectors.toList()),
          source.getString("topic"),
          source.getString("keyFormat"),
          source.getString("valueFormat"),
          formatQueries(source.getJsonArray("readQueries")),
          formatQueries(source.getJsonArray("writeQueries")),
          Optional.ofNullable(emptyToNull(source.getString("timestamp"))),
          Optional.ofNullable(emptyToNull(source.getString("windowType"))),
          source.getString("statement"),
          source.getJsonArray("sourceConstraints", new JsonArray()).stream()
              .map(o -> (String)o)
              .collect(Collectors.toList())
      ));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private static String emptyToNull(final String str) {
    return str == null ? null : (str.isEmpty() ? null : str);
  }
}
