/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicStreamWriterTest {

  @Mock public KafkaConsumer<String, Bytes> kafkaConsumer;
  @Mock public SchemaRegistryClient schemaRegistry;

  @Before
  public void setup() {
    final Iterator<ConsumerRecords<String, Bytes>> records = StreamingTestUtils.generate(
        "topic",
        i -> "key" + i,
        i -> new Bytes(("value" + i).getBytes(Charsets.UTF_8)));
    when(kafkaConsumer.poll(any(Duration.class)))
        .thenAnswer(invocation -> records.next());
  }

  @Test
  public void testIntervalOneAndLimitTwo() {
    // Given:
    final TopicStreamWriter writer = new TopicStreamWriter(
        schemaRegistry,
        kafkaConsumer,
        "topic",
        1,
        Duration.ZERO,
        OptionalInt.of(2)
    );

    // When:
    ValidatingOutputStream out = new ValidatingOutputStream();
    writer.write(out);

    // Then:
    final List<String> expected = ImmutableList.of(
        "Format:STRING",
        "key0 , value0",
        "key1 , value1"
    );
    out.assertWrites(expected);
  }

  @Test
  public void testIntervalTwoAndLimitTwo() {
    // Given:
    final TopicStreamWriter writer = new TopicStreamWriter(
        schemaRegistry,
        kafkaConsumer,
        "topic",
        2,
        Duration.ZERO,
        OptionalInt.of(2)
    );

    // When:
    ValidatingOutputStream out = new ValidatingOutputStream();
    writer.write(out);

    // Then:
    final List<String> expected = ImmutableList.of(
        "Format:STRING",
        "key0 , value0",
        "key2 , value2"
    );
    out.assertWrites(expected);
  }

  private static class ValidatingOutputStream extends OutputStream {

    private final List<byte[]> recordedWrites;

    ValidatingOutputStream() {
      this.recordedWrites = new ArrayList<>();
    }

    @Override public void write(final int b) { /* not called*/ }

    @Override
    public void write(final byte[] b) {
      recordedWrites.add(b);
    }

    void assertWrites(final List<String> expected) {
      assertThat(recordedWrites, hasSize(expected.size()));
      for (int i = 0; i < recordedWrites.size(); i++) {
        final byte[] bytes = recordedWrites.get(i);
        assertThat(
            new String(bytes, Charsets.UTF_8),
            containsString(expected.get(i)));
      }
    }
  }

}