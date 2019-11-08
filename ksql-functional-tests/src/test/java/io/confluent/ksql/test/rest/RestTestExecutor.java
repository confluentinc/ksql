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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlStatementErrorMessage;
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
  private static final Duration MAX_STATIC_WARMUP = Duration.ofSeconds(10);

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
    this.kafkaCluster = Objects.requireNonNull(kafkaCluster, "kafkaCluster");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
  }

  void buildAndExecuteQuery(final RestTestCase testCase) {
    initializeTopics(testCase.getTopics());

    produceInputs(testCase.getInputsByTopic());

    final Optional<List<KsqlEntity>> responses = sendStatements(testCase);
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

  private Optional<List<KsqlEntity>> sendStatements(final RestTestCase testCase) {

    final List<String> allStatements = testCase.getStatements();

    int firstStatic = 0;
    for (; firstStatic < allStatements.size(); firstStatic++) {
      final boolean isStatic = allStatements.get(firstStatic).startsWith("SELECT ");
      if (isStatic) {
        break;
      }
    }

    final List<String> nonStatics = IntStream.range(0, firstStatic)
        .mapToObj(allStatements::get)
        .collect(Collectors.toList());

    final Optional<List<KsqlEntity>> results = sendStatements(testCase, nonStatics);
    if (!results.isPresent()) {
      return Optional.empty();
    }

    final List<String> statics = IntStream.range(firstStatic, allStatements.size())
        .mapToObj(allStatements::get)
        .collect(Collectors.toList());

    if (statics.isEmpty()) {
      failIfExpectingError(testCase);
      return results;
    }

    if (!testCase.expectedError().isPresent()) {
      for (int idx = firstStatic; testCase.getExpectedResponses().size() > idx; ++idx) {
        final String staticStatement = allStatements.get(idx);
        final Response staticResponse = testCase.getExpectedResponses().get(idx);

        waitForWarmStateStores(staticStatement, staticResponse);
      }
    }

    final Optional<List<KsqlEntity>> moreResults = sendStatements(testCase, statics);
    if (!moreResults.isPresent()) {
      return Optional.empty();
    }

    failIfExpectingError(testCase);

    return moreResults
        .map(ksqlEntities -> ImmutableList.<KsqlEntity>builder()
            .addAll(results.get())
            .addAll(ksqlEntities)
            .build());
  }

  private Optional<List<KsqlEntity>> sendStatements(
      final RestTestCase testCase,
      final List<String> statements
  ) {
    final String sql = statements.stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final RestResponse<KsqlEntityList> resp = restClient.makeKsqlRequest(sql);

    if (resp.isErroneous()) {
      final Optional<Matcher<RestResponse<?>>> expectedError = testCase.expectedError();
      if (!expectedError.isPresent()) {
        final String statement = resp.getErrorMessage() instanceof KsqlStatementErrorMessage
            ? ((KsqlStatementErrorMessage)resp.getErrorMessage()).getStatementText()
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
      return Optional.empty();
    }

    return Optional.of(resp.getResponse());
  }

  private void verifyOutput(final RestTestCase testCase) {
    testCase.getOutputsByTopic().forEach((topic, records) -> {
      final Deserializer<?> keyDeserializer =
          topic.getKeyDeserializer(serviceContext.getSchemaRegistryClient(), false);

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

  private static void verifyResponses(
      final List<KsqlEntity> actualResponses,
      final List<Response> expectedResponses,
      final List<String> statements
  ) {
    assertThat(
        "Not enough responses",
        actualResponses,
        hasSize(greaterThanOrEqualTo(expectedResponses.size()))
    );

    for (int idx = 0; idx < expectedResponses.size(); idx++) {
      final Map<String, Object> expected = expectedResponses.get(idx).getContent();
      final Map<String, Object> actual = asJsonMap(actualResponses.get(idx));

      // Expected does not need to include everything, only needs to be tested:
      for (final Entry<String, Object> e : expected.entrySet()) {
        final String key = e.getKey();
        final Object value = replaceMacros(e.getValue(), statements, idx);
        final String baseReason = "Response mismatch at index " + idx;
        assertThat(baseReason, actual, hasKey(key));
        assertThat(baseReason + " on key: " + key, actual.get(key), is(value));
      }
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

  private static Map<String, Object> asJsonMap(final KsqlEntity response) {
    try {
      final ObjectMapper mapper = JsonMapper.INSTANCE.mapper;
      final String text = mapper.writeValueAsString(response);
      return mapper.readValue(text, new TypeReference<Map<String, Object>>() {
      });
    } catch (final Exception e) {
      throw new AssertionError("Failed to serialize response to JSON: " + response);
    }
  }

  private void waitForWarmStateStores(
      final String staticStatement,
      final Response staticResponse
  ) {
    // Special handling for static queries is required, as they depend on materialized state stores
    // being warmed up.  Initial requests may return null values.

    final ImmutableList<Response> expectedResponse = ImmutableList.of(staticResponse);
    final ImmutableList<String> statements = ImmutableList.of(staticStatement);

    final long threshold = System.currentTimeMillis() + MAX_STATIC_WARMUP.toMillis();
    while (System.currentTimeMillis() < threshold) {
      final RestResponse<KsqlEntityList> resp = restClient.makeKsqlRequest(staticStatement);
      if (resp.isErroneous()) {
        Thread.yield();
        LOG.info("Server responded with an error code to a static query. "
            + "This could be because the materialized store is not yet warm.");
        continue;
      }

      final KsqlEntityList actualResponses = resp.getResponse();

      try {
        verifyResponses(actualResponses, expectedResponse, statements);
        return;
      } catch (final AssertionError e) {
        // Potentially, state stores not warm yet
        LOG.info("Server responded with incorrect result to a static query. "
            + "This could be because the materialized store is not yet warm.", e);
        Thread.yield();
      }
    }
  }
}
