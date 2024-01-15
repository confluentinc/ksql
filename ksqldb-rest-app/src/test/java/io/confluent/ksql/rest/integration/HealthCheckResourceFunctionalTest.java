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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.COMMAND_RUNNER_CHECK_NAME;
import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.KAFKA_CHECK_NAME;
import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.METASTORE_CHECK_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.rest.server.state.ServerState.State;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

@Category({IntegrationTest.class})
public class HealthCheckResourceFunctionalTest {

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @Test
  public void shouldCheckHealth() {
    // When:
    final HealthCheckResponse response = RestIntegrationTestUtil.checkServerHealth(REST_APP);

    // Then:
    assertThat("server should be healthy", response.getIsHealthy());
    assertThat(response.getDetails().get(KAFKA_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getDetails().get(METASTORE_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy(), is(true));
    assertThat(response.getServerState(), is(Optional.of(State.READY.toString())));
  }
}