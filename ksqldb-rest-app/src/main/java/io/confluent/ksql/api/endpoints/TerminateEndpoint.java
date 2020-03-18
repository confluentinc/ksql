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

package io.confluent.ksql.api.endpoints;

import io.confluent.ksql.api.spi.EndpointResponse;
import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.VertxUtils;
import javax.ws.rs.core.Response;

public class TerminateEndpoint {

  private final KsqlResource ksqlResource;

  public TerminateEndpoint(final KsqlResource ksqlResource) {
    this.ksqlResource = ksqlResource;
  }

  public EndpointResponse executeTerminate(final KsqlSecurityContext ksqlSecurityContext,
      final ClusterTerminateRequest request) {
    VertxUtils.checkIsWorker();

    final Response response = ksqlResource.terminateCluster(ksqlSecurityContext, request);

    return EndpointResponse.create(response.getStatus(), response.getStatusInfo().getReasonPhrase(),
        response.getEntity());
  }

}
