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


import static io.confluent.ksql.test.util.ThreadTestUtil.filterBuilder;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.loader.TestFile;
import io.confluent.ksql.test.model.TestFileContext;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.util.ThreadTestUtil;
import io.confluent.ksql.test.util.ThreadTestUtil.ThreadSnapshot;
import io.confluent.ksql.test.utils.TestUtils;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.RetryUtil;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.raft.errors.RaftException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

/**
 * Equivalent(ish) to {@code QueryTranslationTest}, but coming in through the Restful API.
 *
 * <p>Note: this is a <b>LOT</b> slower to run that QTT.  Only use for things that can't be tested
 * by QTT. This currently includes:
 *
 * <ul>
 * <li>INSERT VALUES statements</li>
 * <li>Pull queries against materialized stores</li>
 * </ul>
 *
 * Runs the json functional tests defined under `ksql-functional-tests/src/test/resources/rest-query-validation-tests`.
 *
 * See `ksql-functional-tests/README.md` for more info.
 *
 * Note that this test is not directly executable anymore, but has been broken up into batches
 * under the batches package.
 */
public class RestQueryTranslationTest {

  private static final Path TEST_DIR = Paths.get("rest-query-validation-tests");
  private static final String STATIC_TOPIC = "test_topic";

  protected static final TestKsqlRestApp createTestApp(final IntegrationTestHarness testHarness) {
    return TestKsqlRestApp
        .builder(testHarness::kafkaBootstrapServers)
        .withProperty(
            KSQL_STREAMS_PREFIX + StreamsConfig.STATE_DIR_CONFIG,
            TestUtils.tempDirectory().toAbsolutePath().toString()
        )
        .withProperty(
            KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
            StreamsConfig.EXACTLY_ONCE_V2 // To stabilize tests
        )
        // Setting to anything lower will cause the tests to fail because we won't correctly commit
        // transaction marker offsets. This was previously set to 0 to presumably avoid flakiness,
        // so we should keep an eye out for this.
        .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000)
        .withProperty(KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1)
        // The default session timeout was recently increased. Setting to a lower value so that
        // departed nodes are removed from the consumer group quickly.
        .withProperty(KSQL_STREAMS_PREFIX + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000)
        .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "set")
        .withProperty(KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED, true)
        .withProperty(KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED, true)
        .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED, true)
        .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED, true)
        .withProperty(KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_LATEST_DELAY_MS, 0L)
        .withProperty(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED, true)
        .withProperty(KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED, false)
        .withStaticServiceContext(testHarness::getServiceContext)
        .build();
  }

  protected static RuleChain createRuleChain(
      final IntegrationTestHarness testHarness,
      final TestKsqlRestApp restApp) {
    return RuleChain
        .outerRule(Retry.of(3, RaftException.class, 3, TimeUnit.SECONDS))
        .around(testHarness)
        .around(restApp);
  }

  protected static Collection<Object[]> data(int totalSegments, int segment) {
    List<Object[]> collection = JsonTestLoader.of(TEST_DIR, RqttTestFile.class)
        .load()
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toCollection(ArrayList::new));
    int testsPerSegment = collection.size() / totalSegments;
    return collection.subList(testsPerSegment * segment,
        segment < totalSegments - 1 ? testsPerSegment * (segment + 1) : collection.size());
  }

  @Rule
  public final Timeout timeout = Timeout.seconds(90);

  private final RestTestCase testCase;
  private final TestKsqlRestApp app;
  private final IntegrationTestHarness testHarness;
  private final AtomicReference<ThreadSnapshot> startingThreads;

  /**
   * @param name - unused. Is just so the tests get named.
   * @param testCase - testCase to run.
   */
  @SuppressWarnings("unused")
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public RestQueryTranslationTest(final String name, final RestTestCase testCase,
      final TestKsqlRestApp testKsqlRestApp, final IntegrationTestHarness testHarness,
      final AtomicReference<ThreadSnapshot> startingThreads) {
    this.testCase = requireNonNull(testCase, "testCase");
    this.app = testKsqlRestApp;
    this.testHarness = testHarness;
    this.startingThreads = startingThreads;
  }

  @After
  public void tearDown() {
    app.closePersistentQueries();
    app.dropSourcesExcept();


    // Sometimes a race-condition throws an error when deleting a changelog topic (created by
    // a CST query) that is later deleted automatically just before the Kafka API delete is called.
    // Let's retry a few more times before marking this deletion as failure.
    RetryUtil.retryWithBackoff(
        10,
        10,
        (int) TimeUnit.SECONDS.toMillis(10),
        () -> testHarness.getKafkaCluster()
            .deleteAllTopics(TestKsqlRestApp.getCommandTopicName())
    );

    final ThreadSnapshot thread = startingThreads.get();
    if (thread == null) {
      // Only set once one full run completed to ensure all persistent threads created:
      startingThreads.set(ThreadTestUtil.threadSnapshot(filterBuilder()
          .excludeTerminated()
          // There is a pool of ksql worker threads that grows over time, but is capped.
          .nameMatches(name -> !name.startsWith("ksql-workers"))
          // There are two pools for HARouting worker threads that grows over time,
          // but they are capped to 100
          .nameMatches(name -> !name.startsWith("pull-query-coordinator"))
          .nameMatches(name -> !name.startsWith("pull-query-router"))
          .build()));
    } else {
      thread.assertSameThreads();
    }
  }

  protected void shouldBuildAndExecuteQueries() {
    final RestTestCase uniqueTestCase = withUniqueTopicNames(testCase);
    try (RestTestExecutor testExecutor = testExecutor()) {
      testExecutor.buildAndExecuteQuery(uniqueTestCase);
    } catch (final AssertionError | Exception e) {
      throw new AssertionError(e.getMessage()
          + System.lineSeparator()
          + "failed test: " + uniqueTestCase.getName()
          + System.lineSeparator()
          + "in file: " + uniqueTestCase.getTestLocation(),
          e
      );
    }
  }

  private static RestTestCase withUniqueTopicNames(final RestTestCase testCase) {
    final String uniqueSuffix = UUID.randomUUID().toString().substring(0, 8);
    final String uniqueTopic = STATIC_TOPIC + "_" + uniqueSuffix;

    final List<String> statements = testCase.getStatements().stream()
        .map(s -> s.replace(STATIC_TOPIC, uniqueTopic))
        .collect(Collectors.toList());

    final List<Topic> topics = testCase.getTopics().stream()
        .map(t -> new Topic(
            t.getName().replace(STATIC_TOPIC, uniqueTopic),
            t.getNumPartitions(), t.getReplicas(),
            t.getKeySchemaId(), t.getValueSchemaId(),
            t.getKeySchema(), t.getValueSchema(),
            t.getKeySchemaReferences(), t.getValueSchemaReferences(),
            t.getKeyFeatures(), t.getValueFeatures()))
        .collect(Collectors.toList());

    final List<Record> inputRecords = testCase.getInputRecords().stream()
        .map(r -> new Record(
            r.getTopicName().replace(STATIC_TOPIC, uniqueTopic),
            r.rawKey(), r.getJsonKey().orElse(null),
            r.value(), r.getJsonValue().orElse(null),
            r.timestamp(), r.getWindow(), r.headers()))
        .collect(Collectors.toList());

    final List<Record> outputRecords = testCase.getOutputRecords().stream()
        .map(r -> new Record(
            r.getTopicName().replace(STATIC_TOPIC, uniqueTopic),
            r.rawKey(), r.getJsonKey().orElse(null),
            r.value(), r.getJsonValue().orElse(null),
            r.timestamp(), r.getWindow(), r.headers()))
        .collect(Collectors.toList());

    final List<Response> responses = testCase.getExpectedResponses().stream()
        .map(r -> new Response(replaceInMap(r.getContent(), STATIC_TOPIC, uniqueTopic)))
        .collect(Collectors.toList());

    return new RestTestCase(
        testCase.getTestLocation(),
        testCase.getName(),
        testCase.getProperties(),
        topics,
        inputRecords,
        outputRecords,
        statements,
        responses,
        testCase.expectedError(),
        testCase.getInputConditions(),
        testCase.getOutputConditions(),
        testCase.isTestPullWithProtoFormat()
    );
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> replaceInMap(
      final Map<String, Object> map,
      final String oldStr,
      final String newStr
  ) {
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String key = entry.getKey();
      final Object value = replaceInObject(entry.getValue(), oldStr, newStr);
      result.put(key, value);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static Object replaceInObject(
      final Object obj,
      final String oldStr,
      final String newStr
  ) {
    if (obj instanceof String) {
      return ((String) obj).replace(oldStr, newStr);
    } else if (obj instanceof Map) {
      return replaceInMap((Map<String, Object>) obj, oldStr, newStr);
    } else if (obj instanceof List) {
      return ((List<Object>) obj).stream()
          .map(item -> replaceInObject(item, oldStr, newStr))
          .collect(Collectors.toList());
    }
    return obj;
  }

  private RestTestExecutor testExecutor() {
    return new RestTestExecutor(
        app.getEngine(),
        app.getListeners().get(0),
        testHarness.getKafkaCluster(),
        testHarness.getServiceContext()
    );
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class RqttTestFile implements TestFile<RestTestCase> {

    private final List<RestTestCaseNode> tests;

    RqttTestFile(@JsonProperty("tests") final List<RestTestCaseNode> tests) {
      this.tests = ImmutableList.copyOf(requireNonNull(tests, "tests collection missing"));

      if (tests.isEmpty()) {
        throw new IllegalArgumentException("test file did not contain any tests");
      }
    }

    @Override
    public Stream<RestTestCase> buildTests(final TestFileContext ctx) {
      return tests
          .stream()
          .flatMap(node -> RestTestCaseBuilder.buildTests(node, ctx));
    }
  }
}
