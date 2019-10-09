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
import io.confluent.ksql.rest.entity.HealthcheckResponse;
import io.confluent.ksql.rest.entity.HealthcheckResponseDetail;
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

public class HealthcheckAgent {

  private static final List<Check> DEFAULT_CHECKS = ImmutableList.of(
      new Check("metastore", "list streams; list tables; list queries;"),
      new Check("kafka", "list topics extended;")
  );

  private final SimpleKsqlClient ksqlClient;
  private final URI serverEndpoint;

  public HealthcheckAgent(
      final SimpleKsqlClient ksqlClient,
      final KsqlRestConfig restConfig
  ) {
    this.ksqlClient = Objects.requireNonNull(ksqlClient, "ksqlClient");
    this.serverEndpoint = getServerAddress(restConfig);
  }

  public HealthcheckResponse checkHealth() {
    final Map<String, HealthcheckResponseDetail> results = DEFAULT_CHECKS.stream()
        .collect(Collectors.toMap(
            Check::getName,
            check -> new HealthcheckResponseDetail(isSuccessful(check.getKsqlStatement()))
        ));
    final boolean allHealthy = results.values().stream()
        .map(HealthcheckResponseDetail::getIsHealthy)
        .reduce(Boolean::logicalAnd)
        .orElse(true);
    return new HealthcheckResponse(allHealthy, results);
  }

  private boolean isSuccessful(final String ksqlStatement) {
    final RestResponse<KsqlEntityList> response =
        ksqlClient.makeKsqlRequest(serverEndpoint, ksqlStatement);
    return response.isSuccessful();
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

  private static class Check {
    private final String name;
    private final String ksqlStatement;

    Check(final String name, final String ksqlStatement) {
      this.name = Objects.requireNonNull(name, "name");
      this.ksqlStatement = Objects.requireNonNull(ksqlStatement, "ksqlStatement");
    }

    String getName() {
      return name;
    }

    String getKsqlStatement() {
      return ksqlStatement;
    }
  }
}