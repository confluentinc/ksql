/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicStreamWriterTest {

  @Mock
  public KafkaConsumer<Bytes, Bytes> kafkaConsumer;
  @Mock
  public SchemaRegistryClient schemaRegistry;
  private ValidatingOutputStream out;

  @Before
  public void setup() {
    final Iterator<ConsumerRecords<Bytes, Bytes>> records = StreamingTestUtils.generate(
        "topic",
        i -> new Bytes(("key-" + i).getBytes(Charsets.UTF_8)),
        i -> new Bytes(("value-" + i).getBytes(Charsets.UTF_8)));

    when(kafkaConsumer.poll(any(Duration.class)))
        .thenAnswer(invocation -> records.next());

    out = new ValidatingOutputStream();
  }

  @Test
  public void shouldIntervalOneAndLimitTwo() {
    // Given:
    final TopicStreamWriter writer = new TopicStreamWriter(
        schemaRegistry,
        kafkaConsumer,
        "topic",
        1,
        Duration.ZERO,
        OptionalInt.of(2),
        new CompletableFuture<>()
    );

    // When:
    writer.write(out);

    // Then:
    final List<String> expected = ImmutableList.of(
        "Key format: ",
        "KAFKA_STRING",
        System.lineSeparator(),
        "Value format: ",
        "KAFKA_STRING",
        System.lineSeparator(),
        "rowtime: N/A, key: key-0, value: value-0",
        System.lineSeparator(),
        "rowtime: N/A, key: key-1, value: value-1",
        System.lineSeparator()
    );
    out.assertWrites(expected);
  }

  @Test
  public void shouldIntervalTwoAndLimitTwo() {
    // Given:
    final TopicStreamWriter writer = new TopicStreamWriter(
        schemaRegistry,
        kafkaConsumer,
        "topic",
        2,
        Duration.ZERO,
        OptionalInt.of(2),
        new CompletableFuture<>()
    );

    // When:
    final ValidatingOutputStream out = new ValidatingOutputStream();
    writer.write(out);

    // Then:
    final List<String> expected = ImmutableList.of(
        "Key format: ",
        "KAFKA_STRING",
        System.lineSeparator(),
        "Value format: ",
        "KAFKA_STRING",
        System.lineSeparator(),
        "rowtime: N/A, key: key-0, value: value-0",
        System.lineSeparator(),
        "rowtime: N/A, key: key-2, value: value-2",
        System.lineSeparator()
    );
    out.assertWrites(expected);
  }

  private static class ValidatingOutputStream extends OutputStream {

    private final List<String> recordedWrites;

    ValidatingOutputStream() {
      this.recordedWrites = new ArrayList<>();
    }

    @Override public void write(final int b) { /* not called*/ }

    @Override
    public void write(final byte[] bytes, int off, int len) {
      recordedWrites.add(new String(bytes, off, len, Charsets.UTF_8));
    }

    void assertWrites(final List<String> expected) {

      for (int i = 0; i < recordedWrites.size(); i++) {
        final String actual = recordedWrites.get(i);
        if (expected.size() <= i) {
          break;
        }

        assertThat(actual, containsString(expected.get(i)));
      }

      assertThat(recordedWrites, hasSize(expected.size()));
    }
  }
}
