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

package io.confluent.ksql.test.rest;


import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.loader.TestFile;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Equivalent(ish) to {@code QueryTranslationTest}, but coming in through the Restful API.
 *
 * <p>Note: this is a <b>LOT</b> slower to run that QTT.  Only use for things that can't be tested
 * by QTT. This currently includes:
 *
 * <ul>
 * <li>INSERT VALUES statements</li>
 * <li>Static queries against materialized stores</li>
 * </ul>
 *
 * Runs the json functional tests defined under `ksql-functional-tests/src/test/resources/rest-query-validation-tests`.
 *
 * See `ksql-functional-tests/README.md` for more info.
 */
@RunWith(Parameterized.class)
public class RestQueryTranslationTest {

  private static final Path TEST_DIR = Paths.get("rest-query-validation-tests");

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withProperty(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return JsonTestLoader.of(TEST_DIR, RqttTestFile.class)
        .load()
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private final RestTestCase testCase;

  /**
   * @param name - unused. Is just so the tests get named.
   * @param testCase - testCase to run.
   */
  @SuppressWarnings("unused")
  public RestQueryTranslationTest(final String name, final RestTestCase testCase) {
    this.testCase = requireNonNull(testCase, "testCase");
  }

  @After
  public void tearDown() {
    REST_APP.closePersistentQueries();
    REST_APP.dropSourcesExcept();
    TEST_HARNESS.getKafkaCluster().deleteAllTopics(TestKsqlRestApp.getCommandTopicName());
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    try (RestTestExecutor testExecutor = textExecutor()) {
      testExecutor.buildAndExecuteQuery(testCase);
    } catch (final AssertionError e) {
      throw new AssertionError(e.getMessage()
          + System.lineSeparator()
          + "failed test: " + testCase.getName()
          + System.lineSeparator()
          + "in file: " + testCase.getTestFile(),
          e
      );
    }
  }

  private static RestTestExecutor textExecutor() {
    return new RestTestExecutor(
        REST_APP.getListeners().get(0),
        TEST_HARNESS.getKafkaCluster(),
        TEST_HARNESS.getServiceContext()
    );
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class RqttTestFile implements TestFile<RestTestCase> {

    private final List<RestTestCaseNode> tests;
    private final RestTestCaseBuilder builder = new RestTestCaseBuilder();

    RqttTestFile(@JsonProperty("tests") final List<RestTestCaseNode> tests) {
      this.tests = ImmutableList.copyOf(requireNonNull(tests, "tests collection missing"));

      if (tests.isEmpty()) {
        throw new IllegalArgumentException("test file did not contain any tests");
      }
    }

    @Override
    public Stream<RestTestCase> buildTests(final Path testPath) {
      return tests
          .stream()
          .flatMap(node -> builder.buildTests(node, testPath).stream());
    }
  }
}
