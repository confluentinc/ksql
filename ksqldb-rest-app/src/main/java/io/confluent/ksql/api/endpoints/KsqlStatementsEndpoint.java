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
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.VertxUtils;
import java.util.Objects;
import javax.ws.rs.core.Response;

public class KsqlStatementsEndpoint {

  private final KsqlResource ksqlResource;

  public KsqlStatementsEndpoint(final KsqlResource ksqlResource) {
    this.ksqlResource = Objects.requireNonNull(ksqlResource);
  }

  public EndpointResponse executeStatements(final KsqlSecurityContext ksqlSecurityContext,
      final KsqlRequest request) {
    VertxUtils.checkIsWorker();

    final Response response = ksqlResource.handleKsqlStatements(ksqlSecurityContext, request);

    return EndpointResponse.create(response);
  }

}
