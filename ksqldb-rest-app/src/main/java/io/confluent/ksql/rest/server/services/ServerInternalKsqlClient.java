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

package io.confluent.ksql.rest.server.services;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.rest.client.KsqlClientUtil;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.ClusterStatusResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.LagReportingMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

/**
 * A KSQL client implementation that sends requests to KsqlResource directly, rather than going
 * over the network. Used by HealthCheckResource to bypass needing authentication credentials
 * when submitting health check requests.
 */
public class ServerInternalKsqlClient implements SimpleKsqlClient {

  private static final String KSQL_PATH = "/ksql";

  private final KsqlResource ksqlResource;
  private final KsqlSecurityContext securityContext;

  public ServerInternalKsqlClient(
      final KsqlResource ksqlResource,
      final KsqlSecurityContext securityContext
  ) {
    this.ksqlResource = requireNonNull(ksqlResource, "ksqlResource");
    this.securityContext = requireNonNull(securityContext, "securityContext");
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndpoint,
      final String sql
  ) {
    final KsqlRequest request = new KsqlRequest(sql, Collections.emptyMap(), null);
    final Response response = ksqlResource.handleKsqlStatements(securityContext, request);
    return KsqlClientUtil.toRestResponse(
        response,
        KSQL_PATH,
        r -> (KsqlEntityList) r.getEntity()
    );
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndpoint,
      final String sql,
      final Map<String, ?> properties
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void makeAsyncHeartbeatRequest(
      final URI serverEndPoint,
      final KsqlHostInfo host,
      final long timestamp
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RestResponse<ClusterStatusResponse> makeClusterStatusRequest(final URI serverEndPoint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void makeAsyncLagReportRequest(
      final URI serverEndPoint,
      final LagReportingMessage lagReportingMessage
  ) {
    throw new UnsupportedOperationException();
  }
}
