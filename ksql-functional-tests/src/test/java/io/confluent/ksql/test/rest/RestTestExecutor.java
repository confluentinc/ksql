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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.QueryStream;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.ExpectedRecordComparator;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlServerException;
import io.confluent.ksql.util.RetryUtil;
import java.io.Closeable;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestTestExecutor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RestTestExecutor.class);

  private static final String STATEMENT_MACRO = "\\{STATEMENT}";
  private static final Duration MAX_STATIC_WARM_UP = Duration.ofSeconds(30);

  private final KsqlRestClient restClient;
  private final EmbeddedSingleNodeKafkaCluster kafkaCluster;
  private final ServiceContext serviceContext;

  RestTestExecutor(
      final URL url,
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final ServiceContext serviceContext
  ) {
    this.restClient = KsqlRestClient.create(
        url.toString(),
        ImmutableMap.of(),
        ImmutableMap.of(),
        Optional.empty()
    );
    this.kafkaCluster = requireNonNull(kafkaCluster, "kafkaCluster");
    this.serviceContext = requireNonNull(serviceContext, "serviceContext");
  }

  void buildAndExecuteQuery(final RestTestCase testCase) {
    initializeTopics(testCase.getTopics());

    produceInputs(testCase.getInputsByTopic());

    final Optional<List<RqttResponse>> responses = sendStatements(testCase);
    if (!responses.isPresent()) {
      return;
    }

    verifyOutput(testCase);
    verifyResponses(responses.get(), testCase.getExpectedResponses(), testCase.getStatements());
  }

  public void close() {
    restClient.close();
  }

  private void initializeTopics(final List<Topic> topics) {
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

      topic.getSchema().ifPresent(schema -> {
        try {
          serviceContext.getSchemaRegistryClient()
              .register(topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    });
  }

  @SuppressWarnings("unchecked")
  private void produceInputs(final Map<Topic, List<Record>> inputs) {
    inputs.forEach((topic, records) -> {

      try (KafkaProducer<Object, Object> producer = new KafkaProducer<>(
          kafkaCluster.producerConfig(),
          topic.getKeySerializer(serviceContext.getSchemaRegistryClient()),
          topic.getValueSerializer(serviceContext.getSchemaRegistryClient())
      )) {

        records.forEach(record -> producer.send(new ProducerRecord<>(
                topic.getName(),
                null,
                record.timestamp().orElse(0L),
                record.key(),
                record.value()
            ))
        );
      } catch (final Exception e) {
        throw new RuntimeException("Failed to send record to " + topic.getName(), e);
      }
    });
  }

  private Optional<List<RqttResponse>> sendStatements(final RestTestCase testCase) {

    final List<String> allStatements = testCase.getStatements();

    int firstQuery = 0;
    for (; firstQuery < allStatements.size(); firstQuery++) {
      final boolean isQuery = allStatements.get(firstQuery).startsWith("SELECT ");
      if (isQuery) {
        break;
      }
    }

    final List<String> nonQuery = IntStream.range(0, firstQuery)
        .mapToObj(allStatements::get)
        .collect(Collectors.toList());

    final Optional<List<RqttResponse>> adminResults = sendAdminStatements(testCase, nonQuery);
    if (!adminResults.isPresent()) {
      return Optional.empty();
    }

    final List<String> queries = IntStream.range(firstQuery, allStatements.size())
        .mapToObj(allStatements::get)
        .collect(Collectors.toList());

    if (queries.isEmpty()) {
      failIfExpectingError(testCase);
      return adminResults;
    }

    if (!testCase.expectedError().isPresent()) {
      for (int idx = firstQuery; testCase.getExpectedResponses().size() > idx; ++idx) {
        final String queryStatement = allStatements.get(idx);
        final Response queryResponse = testCase.getExpectedResponses().get(idx);

        waitForWarmStateStores(queryStatement, queryResponse);
      }
    }

    final List<RqttResponse> moreResults = sendQueryStatements(testCase, queries);
    if (moreResults.isEmpty()) {
      return Optional.empty();
    }

    failIfExpectingError(testCase);

    return Optional.of(ImmutableList.<RqttResponse>builder()
        .addAll(adminResults.get())
        .addAll(moreResults)
            .build());
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
      final List<String> statements
  ) {
    return statements.stream()
        .map(stmt -> sendQueryStatement(testCase, stmt))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(RqttResponse::query)
        .collect(Collectors.toList());
  }

  private Optional<QueryStream> sendQueryStatement(
      final RestTestCase testCase,
      final String sql
  ) {
    final RestResponse<QueryStream> resp = restClient.makeQueryRequest(sql, null);

    if (resp.isErroneous()) {
      handleErrorResponse(testCase, resp);
      return Optional.empty();
    }

    return Optional.of(resp.getResponse());
  }

  private void verifyOutput(final RestTestCase testCase) {
    testCase.getOutputsByTopic().forEach((topic, records) -> {
      final Deserializer<?> keyDeserializer =
          topic.getKeyDeserializer(serviceContext.getSchemaRegistryClient());

      final Deserializer<?> valueDeserializer =
          topic.getValueDeserializer(serviceContext.getSchemaRegistryClient());

      final List<? extends ConsumerRecord<?, ?>> received = kafkaCluster
          .verifyAvailableRecords(
              topic.getName(),
              records.size(),
              keyDeserializer,
              valueDeserializer
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
      final List<String> statements
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
      actualResponse.verify(expectedType, expectedPayload, statements, idx);
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

    final Object expectedKey = expected.key();
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

  private static <T> T asJson(final Object response, final TypeReference<T> type) {
    try {
      final String text = JsonMapper.INSTANCE.mapper.writeValueAsString(response);
      return JsonMapper.INSTANCE.mapper.readValue(text, type);
    } catch (final Exception e) {
      throw new AssertionError("Failed to serialize response to JSON: " + response);
    }
  }

  private void waitForWarmStateStores(
      final String querySql,
      final Response queryResponse
  ) {
    // Special handling for pull queries is required, as they depend on materialized state stores
    // being warmed up.  Initial requests may return no rows.

    final ImmutableList<Response> expectedResponse = ImmutableList.of(queryResponse);
    final ImmutableList<String> statements = ImmutableList.of(querySql);

    final long threshold = System.currentTimeMillis() + MAX_STATIC_WARM_UP.toMillis();
    while (System.currentTimeMillis() < threshold) {
      final RestResponse<QueryStream> resp = restClient.makeQueryRequest(querySql, null);
      if (resp.isErroneous()) {
        final KsqlErrorMessage errorMessage = resp.getErrorMessage();
        LOG.info("Server responded with an error code to a pull query. "
            + "This could be because the materialized store is not yet warm."
            + System.lineSeparator() + errorMessage);
        threadYield();
        continue;
      }

      final List<RqttResponse> actualResponses = ImmutableList
          .of(RqttResponse.query(resp.getResponse()));

      try {
        verifyResponses(actualResponses, expectedResponse, statements);
        LOG.info("Correct response received");
        return;
      } catch (final AssertionError e) {
        // Potentially, state stores not warm yet
        LOG.info("Server responded with incorrect result to a pull query. "
            + "This could be because the materialized store is not yet warm.", e);
        threadYield();
      }
    }
    LOG.info("Timed out waiting for correct response");
  }

  private static void threadYield() {
    try {
      // More reliable than Thread.yield
      Thread.sleep(1);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  @SuppressWarnings("unchecked")
  private static void matchResponseFields(
      final Map<String, Object> actual,
      final Map<String, Object> expected,
      final List<String> statements,
      final int idx,
      final String path
  ) {
    // Expected does not need to include everything, only keys that need to be tested:
    for (final Entry<String, Object> e : expected.entrySet()) {
      final String expectedKey = e.getKey();
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
            newPath
        );
      } else {
        assertThat("Response mismatch at " + newPath, actualValue, is(expectedValue));
      }
    }
  }

  private interface RqttResponse {

    static List<RqttResponse> admin(final KsqlEntityList adminResponses) {
      return adminResponses.stream()
          .map(RqttAdminResponse::new)
          .collect(Collectors.toList());
    }

    static RqttResponse query(final QueryStream queryStream) {
      final Builder<StreamedRow> responses = ImmutableList.builder();

      while (queryStream.hasNext()) {
        final StreamedRow row = queryStream.next();
        responses.add(row);
      }

      queryStream.close();

      return new RqttQueryResponse(responses.build());
    }

    void verify(
        String expectedType,
        Object expectedPayload,
        List<String> statements,
        int idx
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
        final int idx
    ) {
      assertThat("Expected admin response", expectedType, is("admin"));
      assertThat("Admin payload should be JSON object", expectedPayload, is(instanceOf(Map.class)));

      final Map<String, Object> expected = (Map<String, Object>) expectedPayload;

      final Map<String, Object> actualPayload = asJson(entity, PAYLOAD_TYPE);

      matchResponseFields(actualPayload, expected, statements, idx, "responses[" + idx + "]->admin");
    }
  }

  @VisibleForTesting
  static class RqttQueryResponse implements RqttResponse {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE =
        new TypeReference<Map<String, Object>>() {
        };

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
        final int idx
    ) {
      assertThat("Expected query response", expectedType, is("query"));
      assertThat("Query response should be an array", expectedPayload, is(instanceOf(List.class)));

      final List<?> expectedRows = (List<?>) expectedPayload;

      assertThat(
          "row count mismatch. Got:" + System.lineSeparator() + rows,
          rows.size(),
          is(expectedRows.size())
      );

      for (int i = 0; i != rows.size(); ++i) {
        assertThat(
            "Each row should be JSON object",
            expectedRows.get(i),
            is(instanceOf(Map.class))
        );

        final Map<String, Object> actual = asJson(rows.get(i), PAYLOAD_TYPE);
        final Map<String, Object> expected = (Map<String, Object>) expectedRows.get(i);
        matchResponseFields(actual, expected, statements, idx,
            "responses[" + idx + "]->query[" + i + "]");
      }
    }
  }
}
