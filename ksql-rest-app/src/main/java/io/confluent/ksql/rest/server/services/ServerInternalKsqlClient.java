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
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.Response;

/**
 * A KSQL client implementation that sends requests to KsqlResource directly, rather than going
 * over the network. Used by HealthCheckResource to bypass needing authentication credentials
 * when submitting health check requests.
 */
public class ServerInternalKsqlClient implements SimpleKsqlClient {

  private static final String KSQL_PATH = "/ksql";

  private final KsqlResource ksqlResource;
  private final ServiceContext serviceContext;

  public ServerInternalKsqlClient(
      final KsqlResource ksqlResource,
      final ServiceContext serviceContext
  ) {
    this.ksqlResource = requireNonNull(ksqlResource, "ksqlResource");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndpoint,
      final String sql
  ) {
    final KsqlRequest request = new KsqlRequest(sql, Collections.emptyMap(), null);
    final Response response = ksqlResource.handleKsqlStatements(serviceContext, request);
    return KsqlClientUtil.toRestResponse(
        response,
        KSQL_PATH,
        r -> (KsqlEntityList) r.getEntity()
    );
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndpoint,
      final String sql
  ) {
    throw new UnsupportedOperationException();
  }
}
