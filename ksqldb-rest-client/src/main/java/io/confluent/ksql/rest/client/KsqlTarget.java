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

import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HeartbeatMessage;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.ServerInfo;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientProperties;

@SuppressWarnings("WeakerAccess") // Public API
public final class KsqlTarget {

  private static final int MAX_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(32);

  private static final String STATUS_PATH = "/status";
  private static final String KSQL_PATH = "/ksql";
  private static final String QUERY_PATH = "/query";
  private static final String HEARTBEAT_PATH = "/heartbeat";
  private static final String CLUSTERSTATUS_PATH = "/clusterStatus";
  private static final String LAG_REPORT_PATH = "/lag";

  private final WebTarget target;
  private final LocalProperties localProperties;
  private final Optional<String> authHeader;

  KsqlTarget(
      final WebTarget target,
      final LocalProperties localProperties,
      final Optional<String> authHeader
  ) {
    this.target = requireNonNull(target, "target");
    this.localProperties = requireNonNull(localProperties, "localProperties");
    this.authHeader = requireNonNull(authHeader, "authHeader");
  }

  public KsqlTarget authorizationHeader(final String authHeader) {
    return new KsqlTarget(target, localProperties, Optional.of(authHeader));
  }

  public KsqlTarget properties(final Map<String, ?> properties) {
    return new KsqlTarget(target, new LocalProperties(properties), authHeader);
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return get("/info", ServerInfo.class);
  }

  public RestResponse<HealthCheckResponse> getServerHealth() {
    return get("/healthcheck", HealthCheckResponse.class);
  }

  public Future<Response> postAsyncHeartbeatRequest(
      final KsqlHostInfoEntity host,
      final long timestamp
  ) {
    return postAsync(
        HEARTBEAT_PATH,
        new HeartbeatMessage(host, timestamp),
        Optional.empty()
    );
  }

  public RestResponse<ClusterStatusResponse> getClusterStatus() {
    return get(CLUSTERSTATUS_PATH, ClusterStatusResponse.class);
  }

  public Future<Response> postAsyncLagReportingRequest(
      final LagReportingMessage lagReportingMessage
  ) {
    return postAsync(
        LAG_REPORT_PATH,
        lagReportingMessage,
        Optional.empty()
    );
  }

  public RestResponse<CommandStatuses> getStatuses() {
    return get(STATUS_PATH, CommandStatuses.class);
  }

  public RestResponse<CommandStatus> getStatus(final String commandId) {
    return get(STATUS_PATH + "/" + commandId, CommandStatus.class);
  }

  public RestResponse<KsqlEntityList> postKsqlRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        KSQL_PATH,
        ksqlRequest(ksql, previousCommandSeqNum),
        Optional.empty(),
        true,
        r -> r.readEntity(KsqlEntityList.class)
    );
  }

  public RestResponse<QueryStream> postQueryRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        QUERY_PATH,
        ksqlRequest(ksql, previousCommandSeqNum),
        Optional.of(QueryStream.READ_TIMEOUT_MS),
        false,
        QueryStream::new
    );
  }

  public RestResponse<InputStream> postPrintTopicRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return post(
        QUERY_PATH,
        ksqlRequest(ksql, previousCommandSeqNum),
        Optional.empty(),
        false,
        r -> (InputStream) r.getEntity()
    );
  }

  private KsqlRequest ksqlRequest(
      final String ksql,
      final Optional<Long> previousCommandSeqNum
  ) {
    return new KsqlRequest(
        ksql,
        localProperties.toMap(),
        previousCommandSeqNum.orElse(null)
    );
  }

  private <T> RestResponse<T> get(final String path, final Class<T> type) {
    try (Response response = target
        .path(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .headers(headers())
        .get()
    ) {
      return KsqlClientUtil.toRestResponse(response, path, r -> r.readEntity(type));
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing GET to KSQL server. path:" + path, e);
    }
  }

  private <T> RestResponse<T> post(
      final String path,
      final Object jsonEntity,
      final Optional<Integer> readTimeoutMs,
      final boolean closeResponse,
      final Function<Response, T> mapper
  ) {
    Response response = null;

    try {
      response = target
          .path(path)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .property(ClientProperties.READ_TIMEOUT, readTimeoutMs.orElse(0))
          .headers(headers())
          .post(Entity.json(jsonEntity));

      return KsqlClientUtil.toRestResponse(response, path, mapper);
    } catch (final ProcessingException e) {
      if (shouldRetry(readTimeoutMs, e)) {
        return post(path, jsonEntity, calcReadTimeout(readTimeoutMs), closeResponse, mapper);
      }
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } finally {
      if (response != null && closeResponse) {
        response.close();
      }
    }
  }

  private Future<Response> postAsync(
      final String path,
      final Object jsonEntity,
      final Optional<Integer> readTimeoutMs
  ) {
    try {
      // Performs an asynchronous request
      return target
          .path(path)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .property(ClientProperties.READ_TIMEOUT, readTimeoutMs.orElse(0))
          .headers(headers())
          .async()
          .post(Entity.json(jsonEntity));
    } catch (final ProcessingException e) {
      if (shouldRetry(readTimeoutMs, e)) {
        return postAsync(path, jsonEntity, calcReadTimeout(readTimeoutMs));
      }
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    }
  }

  private MultivaluedMap<String, Object> headers() {
    final MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    authHeader.ifPresent(v -> headers.add(HttpHeaders.AUTHORIZATION, v));
    return headers;
  }

  private static boolean shouldRetry(
      final Optional<Integer> readTimeoutMs,
      final ProcessingException e
  ) {
    return readTimeoutMs.map(timeout -> timeout < MAX_TIMEOUT).orElse(false)
        && e.getCause() instanceof SocketTimeoutException;
  }

  private static Optional<Integer> calcReadTimeout(final Optional<Integer> previousTimeoutMs) {
    return previousTimeoutMs.map(timeout -> Math.min(timeout * 2, MAX_TIMEOUT));
  }
}
