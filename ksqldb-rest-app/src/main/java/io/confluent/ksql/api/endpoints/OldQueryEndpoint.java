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

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.util.VertxUtils;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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
    return streamedQueryResource.streamQuery(ksqlSecurityContext, request, connectionClosedFuture);
  }

}
