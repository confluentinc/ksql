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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.rest.healthcheck.HealthCheckAgent.COMMAND_RUNNER_CHECK_NAME;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.entity.HealthCheckResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class CommandTopicFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @Rule
  public TemporaryFolder backupLocation = new TemporaryFolder();

  private TestKsqlRestApp REST_APP_1;
  private TestKsqlRestApp REST_APP_2;
  
  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);
  }
  
  @Before
  public void setup() {
    REST_APP_1 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withStaticServiceContext(TEST_HARNESS::getServiceContext)
        .withProperty(KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION, backupLocation.getRoot().getAbsolutePath())
        .build();

    REST_APP_2 = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withStaticServiceContext(TEST_HARNESS::getServiceContext)
        .withProperty(KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION, backupLocation.getRoot().getAbsolutePath())
        .build();
  }

  @Test
  public void shouldReturnWarningsInRestResponseWhenDegradedCommandRunner() {
    // When:
    REST_APP_1.start();
    RestIntegrationTestUtil.createStream(REST_APP_1, PAGE_VIEWS_PROVIDER);
    makeKsqlRequest(
        REST_APP_1,
    "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
        + "CREATE STREAM S2 AS SELECT * FROM S;"
    );
    final List<KsqlEntity> results1 = makeKsqlRequest(REST_APP_1, "show streams; show topics; show tables;");
    results1.forEach(ksqlEntity -> {
      assertThat("Warning shouldn't be present in response", ksqlEntity.getWarnings().size() == 0);
    });

    HealthCheckResponse healthCheckResponse = RestIntegrationTestUtil.checkServerHealth(REST_APP_1);
    assertThat(
        "CommandRunner healthcheck is healthy for server 1",
        healthCheckResponse.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy());

    // Delete command topic while server 1 still running
    TEST_HARNESS.deleteTopics(Collections.singletonList(ReservedInternalTopics.commandTopic(new KsqlConfig(Collections.emptyMap()))));

    // Slight delay in warnings appearing since a consumer poll has to complete before CommandRunner becomes DEGRADED
    assertThatEventually(
      "Corrupted warning should be present in response", () -> {
            final List<KsqlEntity> results2 = makeKsqlRequest(REST_APP_1, "show streams; show topics; show tables;");
            return results2.stream().allMatch(ksqlEntity -> {
              final List<KsqlWarning> warnings = ksqlEntity.getWarnings();
              return warnings.size() == 1
                  && warnings.get(0).getMessage().equals(DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_CORRUPTED_ERROR_MESSAGE);
            });
      },
      is(true));

    // need healthcheck cache to expire
    assertThatEventually(
        "CommandRunner healthcheck is unhealthy for server 1",
        () -> {
          final HealthCheckResponse newHealthCheckResponse = RestIntegrationTestUtil.checkServerHealth(REST_APP_1);
          return newHealthCheckResponse.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy();
        }, is(false));

    // Corrupted backup test
    REST_APP_2.start();

    // new DDL that diverges from backup triggers the corruption detection
    final List<KsqlEntity> results3 = makeKsqlRequest(REST_APP_2,
    "CREATE STREAM pageviews_divergent"
        + "(" + PAGE_VIEWS_PROVIDER.ksqlSchemaString(false) + ") "
        + "WITH (kafka_topic='" + PAGE_VIEWS_PROVIDER.topicName() + "', value_format='json');"
    );
    results3.forEach(ksqlEntity -> {
      assertThat("Warning should be present in response",
          ksqlEntity.getWarnings().size() == 1);
      assertThat("Warning is corrupted warning",
          ksqlEntity.getWarnings()
              .get(0).getMessage()
              .equals(DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_CORRUPTED_ERROR_MESSAGE));
    });

    assertThatEventually(
        "CommandRunner healthcheck is unhealthy for server 2",
        () -> {
          final HealthCheckResponse newHealthCheckResponse = RestIntegrationTestUtil.checkServerHealth(REST_APP_2);
          return newHealthCheckResponse.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy();
        }, is(false));
    
    // First server should still be in DEGRADED state even though second server created command topic on start up
    final List<KsqlEntity> results4 = makeKsqlRequest(REST_APP_1, "show streams;");
    results4.forEach(ksqlEntity -> {
      assertThat("Warning should be present in response", ksqlEntity.getWarnings().size() == 1);
      assertThat("Warning isn't corrupted warning",
          ksqlEntity.getWarnings()
              .get(0).getMessage()
              .equals(DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_CORRUPTED_ERROR_MESSAGE));
    });

    healthCheckResponse = RestIntegrationTestUtil.checkServerHealth(REST_APP_1);
    assertThat(
        "CommandRunner healthcheck is unhealthy for server 1",
        !healthCheckResponse.getDetails().get(COMMAND_RUNNER_CHECK_NAME).getIsHealthy());
  }

  private static List<KsqlEntity> makeKsqlRequest(final TestKsqlRestApp restApp, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql);
  }
}