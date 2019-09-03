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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.test.rest.model.Response;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.RetryUtil;
import io.confluent.rest.entities.ErrorMessage;
import java.io.Closeable;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

public class RestTestExecutor implements Closeable {

  private static final String STATEMENT_MACRO = "\\{STATEMENT}";

  private final KsqlRestClient restClient;
  private final EmbeddedSingleNodeKafkaCluster kafkaCluster;
  private final ServiceContext serviceContext;

  RestTestExecutor(
      final URL url,
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final ServiceContext serviceContext
  ) {
    this.restClient = new KsqlRestClient(url.toString());
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
    verifyResponses(testCase, responses.get());
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
          5,
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

        records.forEach(record ->
            producer.send(new ProducerRecord<>(topic.getName(), record.key(), record.value()))
        );
      } catch (final Exception e) {
        throw new RuntimeException("Failed to send record to " + topic.getName(), e);
      }
    });
  }

  private Optional<List<KsqlEntity>> sendStatements(final RestTestCase testCase) {

    final String statements = testCase.getStatements().stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final RestResponse<KsqlEntityList> resp = restClient.makeKsqlRequest(statements);

    if (resp.isErroneous()) {
      final Optional<Matcher<ErrorMessage>> expectedError = testCase.expectedError();
      if (!expectedError.isPresent()) {
        throw new AssertionError(
            "Server failed to execute statement" + System.lineSeparator()
                + "statement: " + System.lineSeparator()
                + "reason: " + resp.getErrorMessage()
        );
      }

      final String reason = "Expected error mismatch."
          + System.lineSeparator()
          + "Actual: " + resp.getErrorMessage();

      assertThat(reason, resp.getErrorMessage(), expectedError.get());
      return Optional.empty();
    }

    testCase.expectedError().map(ee -> {
      throw new AssertionError("Expected last statement to return an error: "
          + StringDescription.toString(ee));
    });

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
      final RestTestCase testCase,
      final List<KsqlEntity> actualResponses
  ) {
    final List<Response> expectedResponses = testCase.getExpectedResponses();

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
        final Object value = replaceMacros(e.getValue(), testCase.getStatements(), idx);
        final String baseReason = "Response mismatch at index " + idx;
        assertThat(baseReason, actual, hasKey(key));
        assertThat(baseReason + " on key: " + key, actual.get(key), is(value));
      }
    }
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
    final Object expectedValue = expected.value();
    final long expectedTimestamp = expected.timestamp().orElse(actualTimestamp);

    final AssertionError error = new AssertionError(
        "Expected <" + expectedKey + ", " + expectedValue + "> "
            + "with timestamp=" + expectedTimestamp
            + " but was <" + actualKey + ", " + actualValue + "> "
            + "with timestamp=" + actualTimestamp);

    if (actualKey != null) {
      if (!actualKey.equals(expectedKey)) {
        throw error;
      }
    } else if (expectedKey != null) {
      throw error;
    }

    if (actualValue != null) {
      if (!actualValue.equals(expectedValue)) {
        throw error;
      }
    } else if (expectedValue != null) {
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
}
