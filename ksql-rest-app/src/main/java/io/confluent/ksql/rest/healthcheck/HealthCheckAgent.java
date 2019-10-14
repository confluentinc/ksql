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
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.rest.RestConfig;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;

public class HealthCheckAgent {

  private static final List<Check> DEFAULT_CHECKS = ImmutableList.of(
      new ExecuteStatementCheck("metastore", "list streams; list tables; list queries;"),
      new ExecuteStatementCheck("kafka", "list topics extended;")
  );

  private final SimpleKsqlClient ksqlClient;
  private final URI serverEndpoint;

  public HealthCheckAgent(
      final SimpleKsqlClient ksqlClient,
      final KsqlRestConfig restConfig
  ) {
    this.ksqlClient = Objects.requireNonNull(ksqlClient, "ksqlClient");
    this.serverEndpoint = getServerAddress(restConfig);
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

  private static URI getServerAddress(final KsqlRestConfig restConfig) {
    final List<String> listeners = restConfig.getList(RestConfig.LISTENERS_CONFIG);
    final String address = listeners.stream()
        .map(String::trim)
        .findFirst()
        .orElseThrow(() -> invalidAddressException(listeners, "value cannot be empty"));

    try {
      return new URL(address).toURI();
    } catch (final Exception e) {
      throw invalidAddressException(listeners, e.getMessage());
    }
  }

  private static RuntimeException invalidAddressException(
      final List<String> serverAddresses,
      final String message
  ) {
    return new ConfigException(RestConfig.LISTENERS_CONFIG, serverAddresses, message);
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