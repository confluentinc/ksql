/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.api.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.DefaultErrorMessages;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.integration.RestIntegrationTestUtil;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.ReservedInternalTopics;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests covering integration tests for migrating command topic to a new kafka
 */
@Category({IntegrationTest.class})
public class CommandTopicMigrationIntegrationTest {
  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
  private static final IntegrationTestHarness OTHER_TEST_HARNESS = IntegrationTestHarness.build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(OTHER_TEST_HARNESS);

  private static TestKsqlRestApp REST_APP;
  private static TestKsqlRestApp NEW_REST_APP;
  private String commandTopic;

  @BeforeClass
  public static void classSetUp() throws Exception {
    REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .build();
    NEW_REST_APP = TestKsqlRestApp
        .builder(TEST_HARNESS::kafkaBootstrapServers)
        .withProperty(KsqlRestConfig.COMMAND_CONSUMER_PREFIX + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, OTHER_TEST_HARNESS.kafkaBootstrapServers())
        .withProperty(KsqlRestConfig.COMMAND_PRODUCER_PREFIX + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, OTHER_TEST_HARNESS.kafkaBootstrapServers())
        .withProperty(KsqlRestConfig.KSQL_COMMAND_TOPIC_MIGRATION_ENABLE, true)
        .build();
  }

  @Before
  public void setup() throws IOException {
    REST_APP.start();

    commandTopic = ReservedInternalTopics.commandTopic(new KsqlConfig(REST_APP.getKsqlRestConfig().getKsqlConfigProperties()));
    TEST_HARNESS.ensureTopics("topic1", "topic2");
  }

  @After
  public void teardown() {
    REST_APP.stop();
    TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
    OTHER_TEST_HARNESS.deleteTopics(Collections.singletonList(commandTopic));
  }

  @Test
  public void shouldMigrateCommandTopic() {
    // Given
    assertFalse(OTHER_TEST_HARNESS.topicExists(commandTopic));
    setUpStreams(commandTopic);

    // When
    NEW_REST_APP.start();

    // Then
    assertThatEventually("Server should be in degraded state", () -> isDegradedState(REST_APP), is(true));
    TEST_HARNESS.verifyAvailableRecords(commandTopic, 5);

    assertTrue(OTHER_TEST_HARNESS.topicExists(commandTopic));
    OTHER_TEST_HARNESS.verifyAvailableRecords(commandTopic, 4);
    final List<String> streamsNames = showStreams(NEW_REST_APP);
    assertThat("Should have TOPIC1", streamsNames.contains("TOPIC1"), is(true));
    assertThat("Should have TOPIC2", streamsNames.contains("TOPIC2"), is(true));
    assertThat("Should have STREAM1", streamsNames.contains("STREAM1"), is(true));
    assertThat("Should have STREAM2", streamsNames.contains("STREAM2"), is(true));
  }

  @Test
  public void shouldMigrateCommandTopicIfTopicExistsAndIsEmpty() {
    // Given
    OTHER_TEST_HARNESS.ensureTopics(commandTopic);
    setUpStreams(commandTopic);

    // When
    NEW_REST_APP.start();

    // Then
    assertThatEventually("Server should be in degraded state", () -> isDegradedState(REST_APP), is(true));
    TEST_HARNESS.verifyAvailableRecords(commandTopic, 5);

    assertTrue(OTHER_TEST_HARNESS.topicExists(commandTopic));
    OTHER_TEST_HARNESS.verifyAvailableRecords(commandTopic, 4);
    final List<String> streamsNames = showStreams(NEW_REST_APP);
    assertThat("Should have TOPIC1", streamsNames.contains("TOPIC1"), is(true));
    assertThat("Should have TOPIC2", streamsNames.contains("TOPIC2"), is(true));
    assertThat("Should have STREAM1", streamsNames.contains("STREAM1"), is(true));
    assertThat("Should have STREAM2", streamsNames.contains("STREAM2"), is(true));
  }

  @Test
  public void shouldNotMigrateCommandTopicIfTopicExistsAndHasRecords() {
    // Given
    OTHER_TEST_HARNESS.ensureTopics(commandTopic);
    OTHER_TEST_HARNESS.produceRecord(commandTopic, "key", "data");
    setUpStreams(commandTopic);

    // When
    NEW_REST_APP.start();

    // Then
    assertThatEventually("Server should be in degraded state", () -> isDegradedState(REST_APP), is(false));
    TEST_HARNESS.verifyAvailableRecords(commandTopic, 4);
  }

  private static void setUpStreams(final String commandTopic) {
    makeKsqlRequest(REST_APP, "CREATE STREAM TOPIC1 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic1', VALUE_FORMAT='JSON');");
    makeKsqlRequest(REST_APP, "CREATE STREAM TOPIC2 (ID INT) "
        + "WITH (KAFKA_TOPIC='topic2', VALUE_FORMAT='JSON');");
    makeKsqlRequest(REST_APP, "CREATE STREAM stream1 AS SELECT * FROM topic1;");
    makeKsqlRequest(REST_APP, "CREATE STREAM stream2 AS SELECT * FROM topic2;");
    TEST_HARNESS.verifyAvailableRecords(commandTopic, 4);
  }

  private static boolean isDegradedState(final TestKsqlRestApp restApp) {
    // If in degraded state, then the following command will return a warning
    final List<KsqlEntity> response = makeKsqlRequest(restApp, "Show Streams;");

    final List<KsqlWarning> warnings = response.get(0).getWarnings();
    return warnings.size() > 0 &&
        (warnings.get(0).getMessage().contains(
            DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_CORRUPTED_ERROR_MESSAGE) ||
            warnings.get(0).getMessage().contains(
                DefaultErrorMessages.COMMAND_RUNNER_DEGRADED_INCOMPATIBLE_COMMANDS_ERROR_MESSAGE));
  }

  private static List<String> showStreams(final TestKsqlRestApp restApp) {
    return ((StreamsList) makeKsqlRequest(restApp, "SHOW STREAMS;").get(0))
        .getStreams().stream().map(SourceInfo::getName).collect(Collectors.toList());
  }

  private static List<KsqlEntity> makeKsqlRequest(final TestKsqlRestApp restApp, final String sql) {
    return RestIntegrationTestUtil.makeKsqlRequest(restApp, sql);
  }
}
