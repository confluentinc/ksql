/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.client;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HeartbeatResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.LagReportingResponse;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.vertx.core.http.HttpClientOptions;
import java.io.Closeable;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class KsqlRestClient implements Closeable {

  private final KsqlClient client;
  private final LocalProperties localProperties;

  private List<URI> serverAddresses;

  /**
   * @param serverAddress the address of the KSQL server to connect to.
   * @param localProps initial set of local properties.
   * @param clientProps properties used to build the client.
   * @param creds optional credentials
   */
  public static KsqlRestClient create(
      final String serverAddress,
      final Map<String, ?> localProps,
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> creds
  ) {
    return create(
        serverAddress,
        localProps,
        clientProps,
        creds,
        (cprops, credz, lprops) -> new KsqlClient(cprops, credz, lprops,
            new HttpClientOptions())
    );
  }

  @VisibleForTesting
  static KsqlRestClient create(
      final String serverAddress,
      final Map<String, ?> localProps,
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> creds,
      final KsqlClientSupplier clientSupplier
  ) {
    final LocalProperties localProperties = new LocalProperties(localProps);
    final KsqlClient client = clientSupplier.get(clientProps, creds, localProperties);
    return new KsqlRestClient(client, serverAddress, localProperties);
  }

  @FunctionalInterface
  interface KsqlClientSupplier {
    KsqlClient get(
        Map<String, String> clientProps,
        Optional<BasicCredentials> creds,
        LocalProperties localProps
    );
  }

  private KsqlRestClient(
      final KsqlClient client,
      final String serverAddress,
      final LocalProperties localProps
  ) {
    this.client = requireNonNull(client, "client");
    this.serverAddresses = parseServerAddresses(serverAddress);
    this.localProperties = requireNonNull(localProps, "localProps");
  }

  public URI getServerAddress() {
    return serverAddresses.get(0);
  }

  public void setServerAddress(final String serverAddress) {
    this.serverAddresses = parseServerAddresses(serverAddress);
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return target().getServerInfo();
  }

  public RestResponse<CommandStatus> getStatus(final String commandId) {
    return target().getStatus(commandId);
  }

  public RestResponse<CommandStatuses> getAllStatuses() {
    return target().getStatuses();
  }

  public RestResponse<ServerMetadata> getServerMetadata() {
    return target().getServerMetadata();
  }

  public RestResponse<ServerClusterId> getServerMetadataId() {
    return target().getServerMetadataId();
  }

  public RestResponse<HealthCheckResponse> getServerHealth() {
    return target().getServerHealth();
  }

  public CompletableFuture<RestResponse<HeartbeatResponse>> makeAsyncHeartbeatRequest(
      final KsqlHostInfoEntity host,
      final long timestamp
  ) {
    return target().postAsyncHeartbeatRequest(host, timestamp);
  }

  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest() {
    return target().getClusterStatus();
  }

  public CompletableFuture<RestResponse<LagReportingResponse>> makeAsyncLagReportingRequest(
      final LagReportingMessage lagReportingMessage
  ) {
    return target().postAsyncLagReportingRequest(lagReportingMessage);
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(final String ksql) {
    return target().postKsqlRequest(ksql, Collections.emptyMap(), Optional.empty());
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(final String ksql, final Long commandSeqNum) {
    return target()
        .postKsqlRequest(ksql, Collections.emptyMap(), Optional.ofNullable(commandSeqNum));
  }

  public RestResponse<CommandStatuses> makeStatusRequest() {
    return target().getStatuses();
  }

  public RestResponse<CommandStatus> makeStatusRequest(final String commandId) {
    return target().getStatus(commandId);
  }

  public RestResponse<StreamPublisher<StreamedRow>> makeQueryRequestStreamed(final String ksql,
      final Long commandSeqNum) {
    return target().postQueryRequestStreamed(ksql, Optional.ofNullable(commandSeqNum));
  }

  public RestResponse<List<StreamedRow>> makeQueryRequest(final String ksql,
      final Long commandSeqNum) {
    return makeQueryRequest(ksql, commandSeqNum, null, Collections.emptyMap());
  }

  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final String ksql,
      final Long commandSeqNum,
      final Map<String, ?> properties,
      final Map<String, ?> requestProperties) {
    KsqlTarget target = target();
    if (properties != null) {
      target = target.properties(properties);
    }
    return target.postQueryRequest(
        ksql, requestProperties, Optional.ofNullable(commandSeqNum));
  }

  public RestResponse<StreamPublisher<String>> makePrintTopicRequest(
      final String ksql,
      final Long commandSeqNum
  ) {
    return target().postPrintTopicRequest(ksql, Optional.ofNullable(commandSeqNum));
  }

  @Override
  public void close() {
    client.close();
  }

  public Object setProperty(final String property, final Object value) {
    return localProperties.set(property, value);
  }

  public Object unsetProperty(final String property) {
    return localProperties.unset(property);
  }

  public Object getProperty(final String property) {
    return localProperties.get(property);
  }

  private KsqlTarget target() {
    return client.target(getServerAddress());
  }

  private static List<URI> parseServerAddresses(final String serverAddresses) {
    requireNonNull(serverAddresses, "serverAddress");
    return ImmutableList.copyOf(
      Arrays.stream(serverAddresses.split(","))
         .map(String::trim)
          .map(KsqlRestClient::parseUri)
         .collect(Collectors.toList()));
  }

  private static URI parseUri(final String serverAddress) {
    try {
      final URL url = new URL(serverAddress);
      if (url.getPort() == -1) {
        return new URL(serverAddress.concat(":") + url.getDefaultPort()).toURI();
      }
      return url.toURI();
    } catch (final Exception e) {
      throw new KsqlRestClientException(
          "The supplied serverAddress is invalid: " + serverAddress, e);
    }
  }

}
