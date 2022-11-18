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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.StreamPublisher;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.integration.QueryStreamSubscriber;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.ExpectedRecordComparator;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.TestCaseBuilderUtil;
import io.confluent.ksql.test.tools.TestJsonMapper;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.TopicInfoCache;
import io.confluent.ksql.test.tools.TopicInfoCache.TopicInfo;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.RetryUtil;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.Closeable;
import java.math.BigDecimal;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.TopologyDescription.Subtopology;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestTestExecutor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RestTestExecutor.class);

  private static final String STATEMENT_MACRO = "\\{STATEMENT}";
  private static final Duration MAX_QUERY_RUNNING_CHECK = Duration.ofSeconds(45);
  private static final Duration MAX_STATIC_WARM_UP = Duration.ofSeconds(45);
  private static final Duration MAX_TOPIC_NAME_LOOKUP = Duration.ofSeconds(45);
  private static final Duration MAX_TRANSIENT_QUERY_COMPLETION_TIME = Duration.ofSeconds(10);
  private static final String MATCH_OPERATOR_DELIMITER = "|";
  private static final String QUERY_KEY = "query";
  private static final String ROW_KEY = "row";
  private static final String COLUMNS_KEY = "columns";

  private final KsqlExecutionContext engine;
  private final KsqlRestClient restClient;
  private final EmbeddedSingleNodeKafkaCluster kafkaCluster;
  private final ServiceContext serviceContext;
  private final TopicInfoCache topicInfoCache;

  RestTestExecutor(
      final KsqlExecutionContext engine,
      final URL url,
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final ServiceContext serviceContext
  ) {
    this.engine = engine;
    this.restClient = KsqlRestClient.create(
        url.toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.empty(),
        Optional.empty()
    );
    this.kafkaCluster = requireNonNull(kafkaCluster, "kafkaCluster");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
    this.topicInfoCache = new TopicInfoCache(engine, serviceContext.getSchemaRegistryClient());
  }

  void buildAndExecuteQuery(final RestTestCase testCase) {
    topicInfoCache.clear();

    if (testCase.getStatements().size() < testCase.getExpectedResponses().size()) {
      throw new AssertionError("Invalid test case: more expected responses than statements. "
          + System.lineSeparator()
          + "statementCount: " + testCase.getStatements().size()
          + System.lineSeparator()
          + "responsesCount: " + testCase.getExpectedResponses().size());
    }

    initializeTopics(testCase);

    final StatementSplit statements = splitStatements(testCase);

    testCase.getProperties().forEach(restClient::setProperty);

    try {
      final Optional<List<RqttResponse>> adminResults =
          sendAdminStatements(testCase, statements.admin);

      if (!adminResults.isPresent()) {
        return;
      }

      final boolean waitForActivePushQueryToProduceInput = testCase.getInputConditions().isPresent()
          && testCase.getInputConditions().get().getWaitForActivePushQuery();
      final Optional<InputConditionsParameters> postInputConditionRunnable;
      if (!waitForActivePushQueryToProduceInput) {
        produceInputs(testCase);
        postInputConditionRunnable = Optional.empty();
      } else {
        postInputConditionRunnable = Optional.of(new InputConditionsParameters(
            this::waitForActivePushQuery, () -> produceInputs(testCase)));
      }

      if (!testCase.expectedError().isPresent()
          && testCase.getExpectedResponses().size() > statements.admin.size()) {
        waitForPersistentQueriesToProcessInputs();
      }

      final List<RqttResponse> queryResults = sendQueryStatements(testCase, statements.queries,
          postInputConditionRunnable);
      if (!queryResults.isEmpty()) {
        failIfExpectingError(testCase);
      }

      final List<RqttResponse> responses = ImmutableList.<RqttResponse>builder()
          .addAll(adminResults.get())
          .addAll(queryResults)
          .build();

      verifyOutput(testCase);
      final boolean verifyOrder = testCase.getOutputConditions().isPresent()
        && testCase.getOutputConditions().get().getVerifyOrder();
      verifyResponses(responses, testCase.getExpectedResponses(), testCase.getStatements(), verifyOrder);

      // Give a few seconds for the transient queries to complete, otherwise, we'll go into teardown
      // and leave the queries stuck.
      waitForTransientQueriesToComplete();

    } finally {
      testCase.getProperties().keySet().forEach(restClient::unsetProperty);
    }
  }

  public void close() {
    restClient.close();
  }

  private void initializeTopics(final RestTestCase testCase) {

    final Collection<Topic> topics = TestCaseBuilderUtil.getAllTopics(
        testCase.getStatements(),
        testCase.getTopics(),
        testCase.getOutputRecords(),
        testCase.getInputRecords(),
        TestFunctionRegistry.INSTANCE.get(),
        new KsqlConfig(testCase.getProperties())
    );

    topics.forEach(topic -> {
      final Runnable createJob = () -> kafkaCluster.createTopic(
          topic.getName(),
          topic.getNumPartitions(),
          topic.getReplicas()
      );

      // Test case could be trying to create a topic deleted by previous test.
      // Need to wait for previous topic to be deleted async, until then requests will fail
      RetryUtil.retryWithBackoff(
          12,
          10,
          (int) TimeUnit.SECONDS.toMillis(10),
          createJob
      );

      topic.getKeySchema().ifPresent(schema -> {
        try {
          serviceContext.getSchemaRegistryClient()
              .register(KsqlConstants.getSRSubject(topic.getName(), true), schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      topic.getValueSchema().ifPresent(schema -> {
        try {
          serviceContext.getSchemaRegistryClient()
              .register(KsqlConstants.getSRSubject(topic.getName(), false), schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    });
  }

  private void produceInputs(final RestTestCase testCase) {
    testCase.getInputsByTopic().forEach((topicName, records) -> {

      final TopicInfo topicInfo = topicInfoCache.get(topicName)
          .orElseThrow(() -> new KsqlException("No information found for topic: " + topicName));

      try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(
          kafkaCluster.producerConfig(),
          topicInfo.getKeySerializer(testCase.getProperties()),
          topicInfo.getValueSerializer(testCase.getProperties())
      )) {
        final List<Future<RecordMetadata>> futures = records.stream()
            .map(record -> new ProducerRecord<>(
                topicName,
                null,
                record.timestamp().orElse(0L),
                record.key(),
                record.value(),
                record.headersAsHeaders().orElse(ImmutableList.of())
            ))
            .map(producer::send)
            .collect(Collectors.toList());

        for (final Future<RecordMetadata> future : futures) {
          future.get();
        }
      } catch (final Exception e) {
        throw new RuntimeException("Failed to send record to " + topicName, e);
      }
    });
  }

  private static StatementSplit splitStatements(final RestTestCase testCase) {

    final List<String> allStatements = testCase.getStatements();

    Integer firstQuery = null;
    for (int idx = 0; idx < allStatements.size(); idx++) {
      final boolean isQuery = allStatements.get(idx).startsWith("SELECT ");
      if (isQuery) {
        if (firstQuery == null) {
          firstQuery = idx;
        }
      } else {
        if (firstQuery != null) {
          throw new AssertionError("Invalid test case: statement " + idx
              + " follows queries, but is not a query. "
              + "All queries should be at the end of the statement list"
          );
        }
      }
    }

    if (firstQuery == null) {
      firstQuery = allStatements.size();
    }

    final List<String> admin = IntStream.range(0, firstQuery)
        .mapToObj(allStatements::get)
        .collect(Collectors.toList());

    final List<String> queries = IntStream.range(firstQuery, allStatements.size())
        .mapToObj(allStatements::get)
        .collect(Collectors.toList());

    return StatementSplit.of(admin, queries);
  }

  private Optional<List<RqttResponse>> sendAdminStatements(
      final RestTestCase testCase,
      final List<String> statements
  ) {
    final String sql = statements.stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final RestResponse<KsqlEntityList> resp = restClient.makeKsqlRequest(sql);

    if (resp.isErroneous()) {
      handleErrorResponse(testCase, resp);
      return Optional.empty();
    }

    final KsqlEntityList entity = resp.getResponse();
    return Optional.of(RqttResponse.admin(entity));
  }

  private List<RqttResponse> sendQueryStatements(
      final RestTestCase testCase,
      final List<String> statements,
      final Optional<InputConditionsParameters> inputConditionsParameters
  ) {
    // We only produce inputs after the first query at the moment to simplify things
    final boolean[] runAfterInputConditions = new boolean[1];
    return statements.stream()
        .map(stmt -> {
          if (inputConditionsParameters.isPresent() && !runAfterInputConditions[0]) {
            runAfterInputConditions[0] = true;
            Optional<List<StreamedRow>> rows =
                sendQueryStatement(testCase, stmt, inputConditionsParameters.get());
            return rows;
          } else if (inputConditionsParameters.isPresent() && runAfterInputConditions[0]) {
            throw new AssertionError(
                "Can only have one query when using inputConditions");
          }
          return sendQueryStatement(testCase, stmt);
        })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(RqttResponse::query)
        .collect(Collectors.toList());
  }

  private Optional<List<StreamedRow>> sendQueryStatement(
      final RestTestCase testCase,
      final String sql
  ) {
    final RestResponse<List<StreamedRow>> resp = restClient.makeQueryRequest(sql, null);

    if (resp.isErroneous()) {
      handleErrorResponse(testCase, resp);
      return Optional.empty();
    }

    return Optional.of(resp.getResponse());
  }

  private Optional<List<StreamedRow>> sendQueryStatement(
      final RestTestCase testCase,
      final String sql,
      final InputConditionsParameters inputConditionsParameters
  ) {
    final RestResponse<StreamPublisher<StreamedRow>> resp
        = restClient.makeQueryRequestStreamed(sql, null);

    if (resp.isErroneous()) {
      handleErrorResponse(testCase, resp);
      return Optional.empty();
    }

    return handleRowPublisher(resp.getResponse(), inputConditionsParameters);
  }

  private Optional<List<StreamedRow>> handleRowPublisher(
      final StreamPublisher<StreamedRow> publisher,
      final InputConditionsParameters inputConditionsParameters
  ) {
    final CompletableFuture<List<StreamedRow>> future = new CompletableFuture<>();
    final CompletableFuture<StreamedRow> header = new CompletableFuture<>();
    final QueryStreamSubscriber subscriber = new QueryStreamSubscriber(publisher.getContext(),
        future, header);
    publisher.subscribe(subscriber);

    try {
      header.get();
      inputConditionsParameters.getWaitForInputConditionsToBeMet().run();
      inputConditionsParameters.getAfterInputConditions().run();
      return Optional.of(future.get());
    } catch (Exception e) {
      LOG.error("Error waiting on header, calling afterHeader, or waiting on rows", e);
      throw new AssertionError(e);
    } finally {
      subscriber.close();
      publisher.close();
    }
  }

  private void waitForActivePushQuery() {
    final long queryRunningThreshold = System.currentTimeMillis()
        + MAX_QUERY_RUNNING_CHECK.toMillis();
    while (System.currentTimeMillis() < queryRunningThreshold) {
      int num = 0;
      for (QueryMetadata queryMetadata : engine.getAllLiveQueries()) {
        if (queryMetadata instanceof TransientQueryMetadata
            && ((TransientQueryMetadata) queryMetadata).isRunning()) {
          num++;
        }
      }
      for (PersistentQueryMetadata queryMetadata : engine.getPersistentQueries()) {
        if (queryMetadata.getScalablePushRegistry().isPresent()
            && queryMetadata.getScalablePushRegistry().get().numRegistered() > 0) {
          num += queryMetadata.getScalablePushRegistry().get().numRegistered();
        }
      }
      if (num == 0) {
        threadYield();
      } else {
        break;
      }
    }
  }

  private void verifyOutput(final RestTestCase testCase) {
    testCase.getOutputsByTopic().forEach((topicName, records) -> {

      final TopicInfo topicInfo = topicInfoCache.get(topicName)
          .orElseThrow(() -> new KsqlException("No information found for topic: " + topicName));

      final List<? extends ConsumerRecord<?, ?>> received = kafkaCluster
          .verifyAvailableRecords(
              topicName,
              records.size(),
              topicInfo.getKeyDeserializer(testCase.getProperties()),
              topicInfo.getValueDeserializer(testCase.getProperties())
          );

      for (int idx = 0; idx < records.size(); idx++) {
        final Record expected = records.get(idx);
        final ConsumerRecord<?, ?> actual = received.get(idx);

        compareKeyValueTimestamp(actual, expected);
      }
    });
  }

  private static void handleErrorResponse(final RestTestCase testCase, final RestResponse<?> resp) {
    final Optional<Matcher<RestResponse<?>>> expectedError = testCase.expectedError();
    if (!expectedError.isPresent()) {
      final String statement = resp.getErrorMessage() instanceof KsqlStatementErrorMessage
          ? ((KsqlStatementErrorMessage) resp.getErrorMessage()).getStatementText()
          : "";

      throw new AssertionError(
          "Server failed to execute statement" + System.lineSeparator()
              + "statement: " + statement + System.lineSeparator()
              + "reason: " + resp.getErrorMessage()
      );
    }

    final String reason = "Expected error mismatch."
        + System.lineSeparator()
        + "Actual: " + resp.getErrorMessage();

    assertThat(reason, resp, expectedError.get());
  }

  private static void verifyResponses(
      final List<RqttResponse> actualResponses,
      final List<Response> expectedResponses,
      final List<String> statements,
      final boolean verifyOrder
  ) {
    assertThat(
        "Not enough responses",
        actualResponses,
        hasSize(greaterThanOrEqualTo(expectedResponses.size()))
    );

    for (int idx = 0; idx < expectedResponses.size(); idx++) {
      final Map<String, Object> expectedResponse = expectedResponses.get(idx).getContent();

      assertThat(expectedResponse.entrySet(), hasSize(1));

      final String expectedType = expectedResponse.keySet().iterator().next();
      final Object expectedPayload = expectedResponse.values().iterator().next();

      final RqttResponse actualResponse = actualResponses.get(idx);
      actualResponse.verify(expectedType, expectedPayload, statements, idx, verifyOrder);
    }
  }

  private static void failIfExpectingError(final RestTestCase testCase) {
    testCase.expectedError().map(ee -> {
      throw new AssertionError("Expected last statement to return an error: "
          + StringDescription.toString(ee));
    });
  }

  private static Object replaceMacros(
      final Object value,
      final List<String> statements,
      final int idx
  ) {
    if (!(value instanceof String)) {
      return value;
    }

    if (statements.size() <= idx) {
      return value;
    }

    final String statement = statements.get(idx);
    return ((String) value).replaceAll(STATEMENT_MACRO, statement);
  }

  private static void compareKeyValueTimestamp(
      final ConsumerRecord<?, ?> actual,
      final Record expected
  ) {
    final long actualTimestamp = actual.timestamp();
    final Object actualKey = actual.key();
    final Object actualValue = actual.value();

    final Object expectedKey = coerceExpectedKey(expected.key(), actualKey);
    final JsonNode expectedValue = expected.getJsonValue()
        .orElseThrow(() -> new KsqlServerException(
            "could not get expected value from test record: " + expected));
    final long expectedTimestamp = expected.timestamp().orElse(actualTimestamp);

    final AssertionError error = new AssertionError(
        "Expected <" + expectedKey + ", " + expectedValue + "> "
            + "with timestamp=" + expectedTimestamp
            + " but was <" + actualKey + ", " + actualValue + "> "
            + "with timestamp=" + actualTimestamp);

    if (!Objects.equals(actualKey, expectedKey)) {
      throw error;
    }

    if (!ExpectedRecordComparator.matches(actualValue, expectedValue)) {
      throw error;
    }

    if (actualTimestamp != expectedTimestamp) {
      throw error;
    }
  }

  /**
   * The expected key loaded from the JSON file may need a little coercing to the right type, e.g
   * a double value of {@code 1.23} will be deserialized as a {@code BigDecimal}.
   * @param expectedKey the key to coerce
   * @param actualKey the type to coerce to.
   * @return the coerced key.
   */
  private static Object coerceExpectedKey(
      final Object expectedKey,
      final Object actualKey
  ) {
    if (actualKey == null || expectedKey == null) {
      return expectedKey;
    }

    if (actualKey instanceof Double && expectedKey instanceof BigDecimal) {
      return ((BigDecimal) expectedKey).doubleValue();
    }

    if (actualKey instanceof Long && expectedKey instanceof Integer) {
      return ((Integer)expectedKey).longValue();
    }

    return expectedKey;
  }

  private static <T> T asJson(final Object response, final TypeReference<T> type) {
    try {
      final String text = TestJsonMapper.INSTANCE.get().writeValueAsString(response);
      return TestJsonMapper.INSTANCE.get().readValue(text, type);
    } catch (final Exception e) {
      throw new AssertionError("Failed to serialize response to JSON: " + response);
    }
  }

  /**
   * This method looks at all of the source topics that feed into all of the subtopologies and waits
   * for the consumer group associated with each application to reach the end offsets for those
   * topics.  This effectively ensures that nothing is lagging when it completes successfully.
   * This should ensure that any materialized state has been built up correctly and is ready for
   * pull queries.
   */
  private void waitForPersistentQueriesToProcessInputs() {
    // First wait for the queries to be in the RUNNING state
    boolean allRunning = false;
    final long queryRunningThreshold = System.currentTimeMillis()
        + MAX_QUERY_RUNNING_CHECK.toMillis();
    while (System.currentTimeMillis() < queryRunningThreshold) {
      boolean notReady = false;
      for (PersistentQueryMetadata persistentQueryMetadata : engine.getPersistentQueries()) {
        if (persistentQueryMetadata.getState() != State.RUNNING) {
          LOG.info("Not all persistent queries are running yet");
          notReady = true;
        }
      }
      if (notReady) {
        threadYield();
      } else {
        allRunning = true;
        LOG.info("All persistent queries are now running");
        break;
      }
    }
    if (!allRunning) {
      throw new AssertionError("Timed out while trying to wait for queries to begin running");
    }

    // Collect all application ids
    List<String> queryApplicationIds = engine.getPersistentQueries().stream()
        .map(QueryMetadata::getQueryApplicationId)
        .collect(Collectors.toList());

    // Collect all possible source topic names for each application id
    Map<String, Set<String>> possibleTopicNamesByAppId = engine.getPersistentQueries().stream()
        .collect(Collectors.toMap(
            QueryMetadata::getQueryApplicationId,
            m -> {
              Set<String> topics = getSourceTopics(m);
              Set<String> possibleInternalNames = topics.stream()
                  .map(t -> m.getQueryApplicationId() + "-" + t)
                  .collect(Collectors.toSet());
              Set<String> all = new HashSet<>();
              all.addAll(topics);
              all.addAll(possibleInternalNames);
              return all;
            }
        ));
    final Set<String> possibleTopicNames = possibleTopicNamesByAppId.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
    // Every topic is either internal or not, so we expect to match exactly half of them.
    int expectedTopics = possibleTopicNames.size() / 2;

    // Find the intersection of possible topic names and real topic names, and wait until the
    // expected number are all there
    final Set<String> topics = new HashSet<>();
    boolean foundTopics = false;
    final long topicThreshold = System.currentTimeMillis() + MAX_TOPIC_NAME_LOOKUP.toMillis();
    while (System.currentTimeMillis() < topicThreshold) {
      Set<String> expectedNames = kafkaCluster.
          getTopics();
      expectedNames.retainAll(possibleTopicNames);
      if (expectedNames.size() == expectedTopics) {
        foundTopics = true;
        topics.addAll(expectedNames);
        LOG.info("All expected topics have now been found");
        break;
      }
    }
    if (!foundTopics) {
      throw new AssertionError("Timed out while trying to find topics");
    }

    // Only retain topic names which are known to exist.
    Map<String, Set<String>> topicNamesByAppId = possibleTopicNamesByAppId.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> {
              e.getValue().retainAll(topics);
              return e.getValue();
            }
        ));

    Map<String, Integer> partitionCount = kafkaCluster.getPartitionCount(topics);
    Map<String, List<TopicPartition>> topicPartitionsByAppId = queryApplicationIds.stream()
        .collect(Collectors.toMap(
            appId -> appId,
            appId -> {
              final List<TopicPartition> allTopicPartitions = new ArrayList<>();
              for (String topic : topicNamesByAppId.get(appId)) {
                for (int i = 0; i < partitionCount.get(topic); i++) {
                  final TopicPartition tp = new TopicPartition(topic, i);
                  allTopicPartitions.add(tp);
                }
              }
              return allTopicPartitions;
            }));

    final long threshold = System.currentTimeMillis() + MAX_STATIC_WARM_UP.toMillis();
    mainloop:
    while (System.currentTimeMillis() < threshold) {
      for (String queryApplicationId : queryApplicationIds) {
        final List<TopicPartition> topicPartitions
            = topicPartitionsByAppId.get(queryApplicationId);

        Map<TopicPartition, Long> currentOffsets =
            kafkaCluster.getConsumerGroupOffset(queryApplicationId);
        Map<TopicPartition, Long> endOffsets = kafkaCluster.getEndOffsets(topicPartitions,
            // Since we're doing At Least Once, we can do read uncommitted.
            IsolationLevel.READ_COMMITTED);

        for (final TopicPartition tp : topicPartitions) {
            if (!currentOffsets.containsKey(tp) && endOffsets.get(tp) > 0) {
              LOG.info("Haven't committed offsets yet for " + tp + " end offset " + endOffsets.get(tp));
              threadYield();
              continue mainloop;
            }
        }

        for (final Map.Entry<TopicPartition, Long> entry : currentOffsets.entrySet()) {
          final TopicPartition tp = entry.getKey();
          final long currentOffset = entry.getValue();
          final long endOffset = endOffsets.get(tp);
          if (currentOffset < endOffset) {
            LOG.info("Offsets are not caught up current: " + currentOffsets + " end: "
                + endOffsets);
            threadYield();
            continue mainloop;
          }
        }
      }
      LOG.info("Offsets are all up to date");
      return;
    }
    LOG.info("Timed out waiting for correct response");
    throw new AssertionError("Timed out while trying to wait for offsets");
  }

  private void waitForTransientQueriesToComplete() {
    // Wait for the transient queries to complete
    final long queryRunningThreshold = System.currentTimeMillis()
        + MAX_TRANSIENT_QUERY_COMPLETION_TIME.toMillis();
    while (System.currentTimeMillis() < queryRunningThreshold) {
      boolean notReady = false;
      // The query is only unregistered after it has been cleaned up.
      for (QueryMetadata queryMetadata : engine.getAllLiveQueries()) {
        if (queryMetadata instanceof TransientQueryMetadata) {
          notReady = true;
        }
      }

      if (notReady) {
        threadYield();
      } else {
        LOG.info("All transient queries have been completed");
        break;
      }
    }
  }

  private Set<String> getSourceTopics(
      final PersistentQueryMetadata persistentQueryMetadata
  ) {
    Set<String> topics = new HashSet<>();
    for (final Subtopology subtopology :
        persistentQueryMetadata.getTopology().describe().subtopologies()) {
      for (final TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof Source) {
          final Source source = (Source) node;
          Preconditions.checkNotNull(source.topicSet(), "Expecting topic set, not regex");
          topics.addAll(source.topicSet());
        }
      }
    }
    return topics;
  }

  private static void threadYield() {
    try {
      // More reliable than Thread.yield
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      // ignore
    }
  }

  private static void verifyResponseFields(final LinkedHashMap<Object, Integer> actualRecords, final LinkedHashMap<Object, Integer> expectedRecords, final HashMap<Object, String> pathIndex){
    for (final Object actualKey : actualRecords.keySet()){
      if (!expectedRecords.containsKey(actualKey)){
        final String reason = pathIndex.get(actualKey);
        if (expectedRecords.size() == 1){
          final Object expectedKey = expectedRecords.keySet().iterator().next();
          assertThat(reason, actualKey, is(expectedKey));
        } else {
          assertThat(reason, actualKey, is(""));
        }
      } else if (!actualRecords.get(actualKey).equals(expectedRecords.get(actualKey))){
        final String reason = "Uneven occurrence of expected vs actual for row " + actualKey;
        assertThat(reason, actualRecords.get(actualKey), is(expectedRecords.get(actualKey)));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void matchResponseFields(
      final Map<String, Object> actual,
      final Map<String, Object> expected,
      final List<String> statements,
      final int idx,
      final String path,
      final HashMap<Object, Integer> actualRecords,
      final HashMap<Object, Integer> expectedRecords,
      final HashMap<Object, String> pathIndex,
      final boolean verifyOrder
  ) {
    // Expected does not need to include everything, only keys that need to be tested:
    for (final Entry<String, Object> e : expected.entrySet()) {
      final int matchIndex = e.getKey().contains(MATCH_OPERATOR_DELIMITER)
          ? e.getKey().indexOf(MATCH_OPERATOR_DELIMITER) : e.getKey().length();
      final String expectedKey = e.getKey().substring(0, matchIndex);
      final MatchOperator operator = e.getKey().contains(MATCH_OPERATOR_DELIMITER)
          ? MatchOperator.valueOf(e.getKey().substring(matchIndex + 1).toUpperCase())
          : MatchOperator.EQUALS;
      final Object expectedValue = replaceMacros(e.getValue(), statements, idx);
      final String baseReason = "Response mismatch at " + path;
      assertThat(baseReason, actual, hasKey(expectedKey));

      final Object actualValue = actual.get(expectedKey);
      final String newPath = path + "->" + expectedKey;

      if (expectedValue instanceof Map) {
        assertThat(actualValue, instanceOf(Map.class));
        matchResponseFields(
            (Map<String, Object>) actualValue,
            (Map<String, Object>) expectedValue,
            statements,
            idx,
            newPath,
            actualRecords,
            expectedRecords,
            pathIndex,
            verifyOrder
        );
      } else {
        if (operator == MatchOperator.STARTS_WITH) {
          assertThat(actualValue, instanceOf(String.class));
          assertThat(expectedValue, instanceOf(String.class));
          assertThat("Response mismatch at " + newPath,
              (String) actualValue, startsWith((String) expectedValue));
        } else {
          final String reason = "Response mismatch at " + newPath;
          if (!verifyOrder && newPath.contains(QUERY_KEY) && newPath.contains(ROW_KEY) && expectedKey.equalsIgnoreCase(COLUMNS_KEY)){
            actualRecords.merge(actualValue, 1, Integer::sum);
            expectedRecords.merge(expectedValue, 1, Integer::sum);
            pathIndex.put(actualValue, reason);
          } else {
            assertThat(reason, actualValue, is(expectedValue));
          }
        }
      }
    }
  }

  private interface RqttResponse {

    static List<RqttResponse> admin(final KsqlEntityList adminResponses) {
      return adminResponses.stream()
          .map(RqttAdminResponse::new)
          .collect(Collectors.toList());
    }

    static RqttResponse query(final List<StreamedRow> rows) {
      return new RqttQueryResponse(rows);
    }

    void verify(
        String expectedType,
        Object expectedPayload,
        List<String> statements,
        int idx,
        boolean verifyOrder
    );
  }

  private static class RqttAdminResponse implements RqttResponse {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE =
        new TypeReference<Map<String, Object>>() {
        };

    private final KsqlEntity entity;

    RqttAdminResponse(final KsqlEntity entity) {
      this.entity = requireNonNull(entity, "entity");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void verify(
        final String expectedType,
        final Object expectedPayload,
        final List<String> statements,
        final int idx,
        final boolean verifyOrder
    ) {
      assertThat("Expected admin response", expectedType, is("admin"));
      assertThat("Admin payload should be JSON object", expectedPayload, is(instanceOf(Map.class)));

      final Map<String, Object> expected = (Map<String, Object>) expectedPayload;

      final Map<String, Object> actualPayload = asJson(entity, PAYLOAD_TYPE);

      final LinkedHashMap<Object, Integer> actualRecords = new LinkedHashMap<>();
      final LinkedHashMap<Object, Integer> expectedRecords = new LinkedHashMap<>();
      final HashMap<Object, String> pathIndex = new HashMap<>();
      matchResponseFields(actualPayload, expected, statements, idx, "responses[" + idx + "]->admin", actualRecords, expectedRecords, pathIndex, verifyOrder);
      verifyResponseFields(actualRecords, expectedRecords, pathIndex);
    }
  }

  @VisibleForTesting
  static class RqttQueryResponse implements RqttResponse {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE =
        new TypeReference<Map<String, Object>>() {
        };

    private static final String INDENT = System.lineSeparator() + "\t";

    private final List<StreamedRow> rows;

    RqttQueryResponse(final List<StreamedRow> rows) {
      this.rows = requireNonNull(rows, "rows");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void verify(
        final String expectedType,
        final Object expectedPayload,
        final List<String> statements,
        final int idx,
        final boolean verifyOrder
    ) {
      assertThat("Expected query response", expectedType, is("query"));
      assertThat("Query response should be an array", expectedPayload, is(instanceOf(List.class)));

      final List<?> expectedRows = (List<?>) expectedPayload;

      assertThat(
          "row count mismatch."
              + System.lineSeparator()
              + "Expected: "
              + expectedRows.stream()
              .map(Object::toString)
              .collect(Collectors.joining(INDENT, INDENT, ""))
              + System.lineSeparator()
              + "Got: "
              + rows.stream()
              .map(Object::toString)
              .collect(Collectors.joining(INDENT, INDENT, ""))
              + System.lineSeparator(),
          rows,
          hasSize(expectedRows.size())
      );

      final LinkedHashMap<Object, Integer> actualRecords = new LinkedHashMap<>();
      final LinkedHashMap<Object, Integer> expectedRecords = new LinkedHashMap<>();
      final HashMap<Object, String> pathIndex = new HashMap<>();
      for (int i = 0; i != rows.size(); ++i) {
        assertThat(
            "Each row should be JSON object",
            expectedRows.get(i),
            is(instanceOf(Map.class))
        );

        final Map<String, Object> actual = asJson(rows.get(i), PAYLOAD_TYPE);
        final Map<String, Object> expected = (Map<String, Object>) expectedRows.get(i);
        matchResponseFields(actual, expected, statements, idx,
          "responses[" + idx + "]->query[" + i + "]",
          actualRecords, expectedRecords, pathIndex, verifyOrder);
      }

      verifyResponseFields(actualRecords, expectedRecords, pathIndex);
    }
  }

  private static final class StatementSplit {

    final List<String> admin;
    final List<String> queries;

    static StatementSplit of(final List<String> admin, final List<String> queries) {
      return new StatementSplit(admin, queries);
    }

    private StatementSplit(final List<String> admin, final List<String> queries) {
      this.admin = ImmutableList.copyOf(admin);
      this.queries = ImmutableList.copyOf(queries);
    }
  }

  private enum MatchOperator {
    EQUALS,
    STARTS_WITH
  }

  private static class InputConditionsParameters {

    private final Runnable waitForInputConditionsToBeMet;
    private final Runnable afterInputConditions;

    public InputConditionsParameters(
        final Runnable waitForInputConditionsToBeMet,
        final Runnable afterInputConditions
    ) {
      this.waitForInputConditionsToBeMet = waitForInputConditionsToBeMet;
      this.afterInputConditions = afterInputConditions;
    }

    public Runnable getWaitForInputConditionsToBeMet() {
      return waitForInputConditionsToBeMet;
    }

    public Runnable getAfterInputConditions() {
      return afterInputConditions;
    }
  }
}
