/*
 * Copyright 2018 Confluent Inc.
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

import com.google.common.base.Suppliers;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.services.KafkaClusterUtil;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.AppInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.function.Supplier;

public class ServerInfoResource {

  private final Supplier<ServerInfo> serverInfo;

  public ServerInfoResource(
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final CommandRunner commandRunner) {
    this.serverInfo = Suppliers.memoize(
        () -> new ServerInfo(
            AppInfo.getVersion(),
            KafkaClusterUtil.getKafkaClusterId(serviceContext),
            ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
            commandRunner.checkCommandRunnerStatus().toString()
        )
    )::get;
  }

  public EndpointResponse get() {
    return EndpointResponse.ok(serverInfo.get());
  }
}
