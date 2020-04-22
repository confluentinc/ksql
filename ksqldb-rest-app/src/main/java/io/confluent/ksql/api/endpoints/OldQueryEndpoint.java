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
import io.confluent.ksql.api.spi.StreamedEndpointResponse;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.VertxUtils;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

/*
This is the ported _old_ API query endpoint
 */
public class OldQueryEndpoint {

  private final StreamedQueryResource streamedQueryResource;

  public OldQueryEndpoint(final StreamedQueryResource streamedQueryResource) {
    this.streamedQueryResource = Objects.requireNonNull(streamedQueryResource);
  }

  public EndpointResponse executeQuery(final KsqlSecurityContext ksqlSecurityContext,
      final KsqlRequest request, final CompletableFuture<Void> connectionClosedFuture) {
    VertxUtils.checkIsWorker();

    final Response response = streamedQueryResource
        .streamQuery(ksqlSecurityContext, request, connectionClosedFuture);

    if (response.getEntity() instanceof StreamingOutput) {
      return StreamedEndpointResponse.create(((StreamingOutput) response.getEntity()));
    } else {
      return EndpointResponse.create(response);
    }


  }

}
