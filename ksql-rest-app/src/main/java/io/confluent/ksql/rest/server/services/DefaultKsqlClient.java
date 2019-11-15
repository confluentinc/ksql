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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.properties.LocalProperties;
import io.confluent.ksql.rest.client.KsqlClient;
import io.confluent.ksql.rest.client.KsqlTarget;
import io.confluent.ksql.rest.client.QueryStream;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.SimpleKsqlClient;
import java.net.URI;
import java.util.List;
import java.util.Optional;

final class DefaultKsqlClient implements SimpleKsqlClient {

  private final Optional<String> authHeader;
  private final KsqlClient sharedClient;

  DefaultKsqlClient(final Optional<String> authHeader) {
    this(
        authHeader,
        new KsqlClient(
            ImmutableMap.of(),
            Optional.empty(),
            new LocalProperties(ImmutableMap.of())
        )
    );
  }

  @VisibleForTesting
  DefaultKsqlClient(
      final Optional<String> authHeader,
      final KsqlClient sharedClient
  ) {
    this.authHeader = requireNonNull(authHeader, "authHeader");
    this.sharedClient = requireNonNull(sharedClient, "sharedClient");
  }

  @Override
  public RestResponse<KsqlEntityList> makeKsqlRequest(
      final URI serverEndPoint,
      final String sql
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    return authHeader
        .map(target::authorizationHeader)
        .orElse(target)
        .postKsqlRequest(sql, Optional.empty());
  }

  @Override
  public RestResponse<List<StreamedRow>> makeQueryRequest(
      final URI serverEndPoint,
      final String sql
  ) {
    final KsqlTarget target = sharedClient
        .target(serverEndPoint);

    final RestResponse<QueryStream> resp = authHeader
        .map(target::authorizationHeader)
        .orElse(target)
        .postQueryRequest(sql, Optional.empty());

    if (resp.isErroneous()) {
      return RestResponse.erroneous(resp.getStatusCode(), resp.getErrorMessage());
    }

    final QueryStream stream = resp.getResponse();

    final Builder<StreamedRow> rows = ImmutableList.builder();
    while (stream.hasNext()) {
      rows.add(stream.next());
    }

    return RestResponse.successful(resp.getStatusCode(), rows.build());
  }
}
