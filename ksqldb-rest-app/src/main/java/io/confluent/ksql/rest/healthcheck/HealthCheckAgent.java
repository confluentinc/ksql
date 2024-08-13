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
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.HealthCheckResponseDetail;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.ServerUtil;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.state.ServerState.State;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthCheckAgent {
  private static final Logger log = LoggerFactory.getLogger(HealthCheckAgent.class);

  public static final String METASTORE_CHECK_NAME = "metastore";
  public static final String KAFKA_CHECK_NAME = "kafka";
  public static final String COMMAND_RUNNER_CHECK_NAME = "commandRunner";

  private static final List<Check> DEFAULT_CHECKS = ImmutableList.of(
      new ExecuteStatementCheck(METASTORE_CHECK_NAME, "list streams; list tables; list queries;"),
      new KafkaBrokerCheck(KAFKA_CHECK_NAME),
      new CommandRunnerCheck(COMMAND_RUNNER_CHECK_NAME)
  );

  private final SimpleKsqlClient ksqlClient;
  private final URI serverEndpoint;
  private final KsqlConfig ksqlConfig;
  private final CommandRunner commandRunner;
  private final Admin adminClient;

  public HealthCheckAgent(
      final SimpleKsqlClient ksqlClient,
      final KsqlRestConfig restConfig,
      final KsqlConfig ksqlConfig,
      final CommandRunner commandRunner,
      final Admin adminClient
  ) {
    this.ksqlClient = Objects.requireNonNull(ksqlClient, "ksqlClient");
    this.serverEndpoint = ServerUtil.getServerAddress(restConfig);
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.commandRunner = Objects.requireNonNull(commandRunner, "commandRunner");
    this.adminClient = Objects.requireNonNull(adminClient, "adminClient");
  }

  public HealthCheckResponse checkHealth() {
    final Map<String, HealthCheckResponseDetail> results = DEFAULT_CHECKS.stream()
        .collect(Collectors.toMap(
            Check::getName,
            check -> check.check(this)
        ));
    final boolean allHealthy = results.values().stream()
        .allMatch(HealthCheckResponseDetail::getIsHealthy);
    final State serverState = commandRunner.checkServerState();
    return new HealthCheckResponse(allHealthy, results, Optional.of(serverState.toString()));
  }

  private interface Check {
    String getName();

    HealthCheckResponseDetail check(HealthCheckAgent healthCheckAgent);
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
    public HealthCheckResponseDetail check(final HealthCheckAgent healthCheckAgent) {
      final RestResponse<KsqlEntityList> response =
          healthCheckAgent.ksqlClient
              .makeKsqlRequest(
                  healthCheckAgent.serverEndpoint,
                  ksqlStatement,
                  ImmutableMap.of(KsqlRequestConfig.KSQL_REQUEST_INTERNAL_REQUEST, true));
      return new HealthCheckResponseDetail(response.isSuccessful());
    }
  }

  private static class KafkaBrokerCheck implements Check {
    private static final int DESCRIBE_TOPICS_TIMEOUT_MS = 30000;

    private final String name;

    KafkaBrokerCheck(final String name) {
      this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public String getName() {
      return name;
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Override
    public HealthCheckResponseDetail check(final HealthCheckAgent healthCheckAgent) {
      final String commandTopic = ReservedInternalTopics.commandTopic(healthCheckAgent.ksqlConfig);
      boolean isHealthy;

      try {
        log.info("Checking ksql's ability to contact broker");
        healthCheckAgent.adminClient
            .describeTopics(Collections.singletonList(commandTopic),
                new DescribeTopicsOptions().timeoutMs(DESCRIBE_TOPICS_TIMEOUT_MS))
            .allTopicNames()
            .get();

        isHealthy = true;
      } catch (final KsqlTopicAuthorizationException e) {
        log.info("ksqlDB denied access to describe cmd topic. This is considered healthy");
        isHealthy = true;
      } catch (final Exception e) {
        log.error("Error describing command topic during health check", e);
        isHealthy = e instanceof UnknownTopicOrPartitionException
            || ExceptionUtils.getRootCause(e) instanceof UnknownTopicOrPartitionException;
      }

      return new HealthCheckResponseDetail(isHealthy);
    }
  }

  private static class CommandRunnerCheck implements Check {
    private final String name;

    CommandRunnerCheck(final String name) {
      this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public HealthCheckResponseDetail check(final HealthCheckAgent healthCheckAgent) {
      return new HealthCheckResponseDetail(
          healthCheckAgent.commandRunner.checkCommandRunnerStatus()
              == CommandRunner.CommandRunnerStatus.RUNNING);
    }
  }
}