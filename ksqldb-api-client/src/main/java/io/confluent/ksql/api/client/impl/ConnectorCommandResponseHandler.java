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

package io.confluent.ksql.api.client.impl;

import static io.confluent.ksql.api.client.impl.AdminResponseHandlers.isCreateConnectorResponse;
import static io.confluent.ksql.api.client.impl.AdminResponseHandlers.isDropConnectorResponse;

import io.confluent.ksql.api.client.ConnectorDescription;
import io.confluent.ksql.api.client.ConnectorInfo;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class ConnectorCommandResponseHandler {

  private ConnectorCommandResponseHandler() {
  }

  static void handleCreateConnectorResponse(
      final JsonObject connectorInfoEntity,
      final CompletableFuture<Void> cf
  ) {
    if (isCreateConnectorResponse(connectorInfoEntity)) {
      cf.complete(null);
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + connectorInfoEntity
      ));
    }
  }

  static void handleDropConnectorResponse(
      final JsonObject dropConnectorResponseEntity,
      final CompletableFuture<Void> cf
  ) {
    if (isDropConnectorResponse(dropConnectorResponseEntity)) {
      cf.complete(null);
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + dropConnectorResponseEntity
      ));
    }
  }

  static void handleListConnectorsResponse(
      final JsonObject connectorsListEntity,
      final CompletableFuture<List<ConnectorInfo>> cf
  ) {
    final Optional<List<ConnectorInfo>> connectors =
        getListConnectorsResponse(connectorsListEntity);
    if (connectors.isPresent()) {
      cf.complete(connectors.get());
    } else {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + connectorsListEntity));
    }
  }

  @SuppressWarnings("unchecked")
  static void handleDescribeConnectorsResponse(
      final JsonObject connectorDescriptionEntity,
      final CompletableFuture<ConnectorDescription> cf
  ) {
    try {
      final JsonObject status = connectorDescriptionEntity.getJsonObject("status");
      cf.complete(new ConnectorDescriptionImpl(
          status.getString("name"),
          connectorDescriptionEntity.getString("connectorClass"),
          connectorDescriptionEntity.getJsonArray("sources").getList(),
          connectorDescriptionEntity.getJsonArray("topics").getList(),
          new ConnectorTypeImpl(status.getString("type")),
          status.getJsonObject("connector").getString("state")
      ));
    } catch (Exception e) {
      cf.completeExceptionally(new IllegalStateException(
          "Unexpected server response format. Response: " + connectorDescriptionEntity));
    }
  }

  /**
   * Attempts to parse the provided response entity as a {@code List<ConnectorInfo>}.
   *
   * @param connectorsEntity response entity
   * @return optional containing parsed result if successful, else empty
   */
  private static Optional<List<ConnectorInfo>> getListConnectorsResponse(
      final JsonObject connectorsEntity) {
    try {
      final JsonArray connectors = connectorsEntity.getJsonArray("connectors");
      return Optional.of(connectors.stream()
          .map(o -> (JsonObject) o)
          .map(o -> new ConnectorInfoImpl(
              o.getString("name"),
              new ConnectorTypeImpl(o.getString("type")),
              o.getString("className"),
              o.getString("state")))
          .collect(Collectors.toList()));
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
