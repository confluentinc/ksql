/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.services.KafkaClusterUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Objects;

public final class ServerMetadataResource {

  private final ServerMetadata serverMetadata;

  private ServerMetadataResource(final ServerMetadata serverMetadata) {
    this.serverMetadata = Objects.requireNonNull(serverMetadata, "serverMetadata");
  }

  public EndpointResponse getServerMetadata() {
    return EndpointResponse.ok(serverMetadata);
  }

  public EndpointResponse getServerClusterId() {
    return EndpointResponse.ok(serverMetadata.getClusterId());
  }

  public static ServerMetadataResource create(
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig
  ) {
    return new ServerMetadataResource(new ServerMetadata(
        AppInfo.getVersion(),
        ServerClusterId.of(
            KafkaClusterUtil.getKafkaClusterId(serviceContext),
            ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
        )
    ));
  }
}
