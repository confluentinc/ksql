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
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
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
import io.vertx.core.http.HttpVersion;
import java.io.Closeable;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class KsqlRestClient implements Closeable {

  static final String CCLOUD_CONNECT_USERNAME_HEADER = "X-Confluent-API-Key";
  static final String CCLOUD_CONNECT_PASSWORD_HEADER = "X-Confluent-API-Secret";

  private final KsqlClient client;
  private final LocalProperties localProperties;
  private final AtomicReference<String> serializedConsistencyVector;
  private final Optional<BasicCredentials> ccloudApiKey;

  private List<URI> serverAddresses;
  private boolean isCCloudServer;



  /**
   * @param serverAddress the address of the KSQL server to connect to.
   * @param localProps initial set of local properties.
   * @param clientProps properties used to build the client.
   * @param creds optional credentials
   */
  @Deprecated
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
        Optional.empty(),
        (cprops, credz, lprops) -> new KsqlClient(cprops, credz, lprops,
            new HttpClientOptions(),
            Optional.of(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)))
    );
  }

  /**
   * @param serverAddress the address of the KSQL server to connect to.
   * @param localProps initial set of local properties.
   * @param clientProps properties used to build the client.
   * @param creds optional credentials
   * @param ccloudApiKey optional Confluent Cloud credentials for connector management
   *                     from the ksqlDB CLI
   */
  public static KsqlRestClient create(
      final String serverAddress,
      final Map<String, ?> localProps,
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> creds,
      final Optional<BasicCredentials> ccloudApiKey
  ) {
    return create(
        serverAddress,
        localProps,
        clientProps,
        creds,
        ccloudApiKey,
        (cprops, credz, lprops) -> new KsqlClient(cprops, credz, lprops,
            new HttpClientOptions(),
            Optional.of(new HttpClientOptions().setProtocolVersion(HttpVersion.HTTP_2)))
    );
  }

  @VisibleForTesting
  static KsqlRestClient create(
      final String serverAddress,
      final Map<String, ?> localProps,
      final Map<String, String> clientProps,
      final Optional<BasicCredentials> creds,
      final Optional<BasicCredentials> ccloudApiKey,
      final KsqlClientSupplier clientSupplier
  ) {
    final LocalProperties localProperties = new LocalProperties(localProps);
    final KsqlClient client = clientSupplier.get(clientProps, creds, localProperties);
    return new KsqlRestClient(client, serverAddress, localProperties, ccloudApiKey);
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
      final LocalProperties localProps,
      final Optional<BasicCredentials> ccloudApiKey
  ) {
    this.client = requireNonNull(client, "client");
    this.serverAddresses = parseServerAddresses(serverAddress);
    this.localProperties = requireNonNull(localProps, "localProps");
    this.ccloudApiKey = ccloudApiKey;
    this.serializedConsistencyVector = new AtomicReference<>();
    this.isCCloudServer = false;
  }

  public URI getServerAddress() {
    return serverAddresses.get(0);
  }

  public boolean getIsCCloudServer() {
    return isCCloudServer;
  }

  public boolean getHasCCloudApiKey() {
    return ccloudApiKey.isPresent();
  }

  public void setServerAddress(final String serverAddress) {
    this.serverAddresses = parseServerAddresses(serverAddress);
  }

  public void setIsCCloudServer(final boolean isCCloudServer) {
    this.isCCloudServer = isCCloudServer;
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

  public RestResponse<KsqlEntityList> makeConnectorRequest(
      final String ksql,
      final Long commandSeqNum
  ) {
    return target(true)
        .postKsqlRequest(ksql, Collections.emptyMap(), Optional.ofNullable(commandSeqNum));
  }

  public RestResponse<CommandStatuses> makeStatusRequest() {
    return target().getStatuses();
  }

  public RestResponse<CommandStatus> makeStatusRequest(final String commandId) {
    return target().getStatus(commandId);
  }

  public RestResponse<Boolean> makeIsValidRequest(final String propertyName) {
    return target().getIsValidRequest(propertyName);
  }

  public RestResponse<StreamPublisher<StreamedRow>> makeQueryRequestStreamed(
      final String ksql,
      final Long commandSeqNum
  ) {
    return makeQueryRequestStreamed(ksql, commandSeqNum, null);
  }

  public RestResponse<StreamPublisher<StreamedRow>> makeQueryRequestStreamed(
      final String ksql,
      final Long commandSeqNum,
      final Map<String, ?> properties
  ) {
    return makeQueryRequestStreamed(ksql, commandSeqNum, properties, Collections.emptyMap());
  }

  public RestResponse<StreamPublisher<StreamedRow>> makeQueryRequestStreamed(
      final String ksql,
      final Long commandSeqNum,
      final Map<String, ?> properties,
      final Map<String, ?> requestProperties
  ) {
    KsqlTarget target = target();
    final Map<String, Object> requestPropertiesToSend = setConsistencyVector(requestProperties);
    if (properties != null) {
      target = target.properties(properties);
    }
    final RestResponse<StreamPublisher<StreamedRow>> response = target.postQueryRequestStreamed(
        ksql, requestPropertiesToSend, Optional.ofNullable(commandSeqNum));
    return response;
  }

  @VisibleForTesting
  public CompletableFuture<RestResponse<StreamPublisher<StreamedRow>>>
      makeQueryRequestStreamedAsync(
      final String ksql,
      final Map<String, ?> properties
  ) {
    KsqlTarget targetHttp2 = targetHttp2();
    if (!properties.isEmpty()) {
      targetHttp2 = targetHttp2.properties(properties);
    }
    return targetHttp2.postQueryRequestStreamedAsync(ksql, properties);
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
    final Map<String, Object> requestPropertiesToSend = setConsistencyVector(requestProperties);
    return target.postQueryRequest(
        ksql, requestPropertiesToSend, Optional.ofNullable(commandSeqNum));
  }

  public RestResponse<List<StreamedRow>> makeQueryStreamRequestProto(
          final String ksql,
          final Map<String, Object> requestProperties
  ) {
    final KsqlTarget target = target();
    return target.postQueryStreamRequestProto(ksql, requestProperties);
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

  @VisibleForTesting
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "should be mutable")
  public AtomicReference<String> getSerializedConsistencyVector() {
    return serializedConsistencyVector;
  }

  private KsqlTarget target() {
    return target(false);
  }

  private KsqlTarget target(final boolean includeConnectorHeaders) {
    final Map<String, String> additionalHeaders = includeConnectorHeaders && isCCloudServer
        ? ccloudConnectorHeaders()
        : Collections.emptyMap();
    return client.target(getServerAddress(), additionalHeaders);
  }

  private KsqlTarget targetHttp2() {
    return client.targetHttp2(getServerAddress());
  }

  private Map<String, String> ccloudConnectorHeaders() {
    if (!ccloudApiKey.isPresent()) {
      throw new IllegalStateException("Should not request headers if no credentials provided.");
    }
    return ImmutableMap.of(
        CCLOUD_CONNECT_USERNAME_HEADER, ccloudApiKey.get().username(),
        CCLOUD_CONNECT_PASSWORD_HEADER, ccloudApiKey.get().password()
    );
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
        return new URL(
                url.getProtocol(),
                url.getHost(),
                url.getDefaultPort(),
                url.getFile()
        ).toURI();
      }
      return url.toURI();
    } catch (final Exception e) {
      throw new KsqlRestClientException(
          "The supplied serverAddress is invalid: " + serverAddress, e);
    }
  }

  private Map<String, Object> setConsistencyVector(
      final Map<String, ?> requestProperties
  ) {
    final Map<String, Object> requestPropertiesToSend = new HashMap<>();
    if (requestProperties != null) {
      requestPropertiesToSend.putAll(requestProperties);
    }
    return requestPropertiesToSend;
  }
}
