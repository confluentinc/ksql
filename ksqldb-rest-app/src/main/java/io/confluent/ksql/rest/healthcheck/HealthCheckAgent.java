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

package io.confluent.ksql.rest.healthcheck;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponseDetail;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.services.SimpleKsqlClient;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class HealthCheckAgent {

  public static final String METASTORE_CHECK_NAME = "metastore";
  public static final String KAFKA_CHECK_NAME = "kafka";

  private static final List<Check> DEFAULT_CHECKS = ImmutableList.of(
      new ExecuteStatementCheck(METASTORE_CHECK_NAME, "list streams; list tables; list queries;"),
      new ExecuteStatementCheck(KAFKA_CHECK_NAME, "list topics;")
  );

  private final SimpleKsqlClient ksqlClient;
  private final URI serverEndpoint;

  public HealthCheckAgent(
      final SimpleKsqlClient ksqlClient,
      final KsqlRestConfig restConfig
  ) {
    this.ksqlClient = Objects.requireNonNull(ksqlClient, "ksqlClient");
    this.serverEndpoint = ServerUtil.getServerAddress(restConfig);
  }

  public HealthCheckResponse checkHealth() {
    final Map<String, HealthCheckResponseDetail> results = DEFAULT_CHECKS.stream()
        .collect(Collectors.toMap(
            Check::getName,
            check -> check.check(ksqlClient, serverEndpoint)
        ));
    final boolean allHealthy = results.values().stream()
        .allMatch(HealthCheckResponseDetail::getIsHealthy);
    return new HealthCheckResponse(allHealthy, results);
  }

  private interface Check {
    String getName();

    HealthCheckResponseDetail check(SimpleKsqlClient ksqlClient, URI serverEndpoint);
  }

  private static class ExecuteStatementCheck implements Check {
    private final String name;
    private final String ksqlStatement;

    ExecuteStatementCheck(final String name, final String ksqlStatement) {
      this.name = Objects.requireNonNull(name, "name");
      this.ksqlStatement = Objects.requireNonNull(ksqlStatement, "ksqlStatement");
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public HealthCheckResponseDetail check(
        final SimpleKsqlClient ksqlClient,
        final URI serverEndpoint
    ) {
      final RestResponse<KsqlEntityList> response =
          ksqlClient.makeKsqlRequest(serverEndpoint, ksqlStatement);
      return new HealthCheckResponseDetail(response.isSuccessful());
    }
  }
}