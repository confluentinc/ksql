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

package io.confluent.ksql.test.tools;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.test.util.EmbeddedSingleNodeKafkaCluster;
import java.io.Closeable;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class RestTestExecutor implements Closeable {

  private final KsqlRestClient restClient;
  private final EmbeddedSingleNodeKafkaCluster kafkaCluster;
  private final SchemaRegistryClient srClient;

  public RestTestExecutor(
      final URL url,
      final EmbeddedSingleNodeKafkaCluster kafkaCluster,
      final SchemaRegistryClient srClient
  ) {
    this.restClient = new KsqlRestClient(url.toString());
    this.kafkaCluster = Objects.requireNonNull(kafkaCluster, "kafkaCluster");
    this.srClient = Objects.requireNonNull(srClient, "srClient");
  }

  public void buildAndExecuteQuery(final TestCase testCase) {
    if (!sendStatements(testCase)) {
      return;
    }

    if (testCase.isAnyExceptionExpected()) {
      throw testCase.failDueToMissingExceptionError();
    }

    verifyOutput(testCase);
  }

  private boolean sendStatements(final TestCase testCase) {
    try {
      final String statements = testCase.statements().stream()
          .collect(Collectors.joining(System.lineSeparator()));

      final RestResponse<KsqlEntityList> resp = restClient.makeKsqlRequest(statements);
      if (resp.isErroneous()) {
        throw new AssertionError(
            "Server failed to execture statement" + System.lineSeparator()
                + "statement: " + System.lineSeparator()
                + "reason: " + resp.getErrorMessage()
        );
      }

      return true;
    } catch (final RuntimeException e) {
      testCase.handleException(e);
      return false;
    }
  }

  private void verifyOutput(final TestCase testCase) {
    testCase.getOutputRecordsByTopic().forEach((topic, records) -> {
      final Deserializer<?> keyDeserializer = topic.getKeyDeserializer(srClient, false);
      final Deserializer<?> valueDeserializer = topic.getValueDeserializer(srClient);

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

  public void close() {
    restClient.close();
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
}
