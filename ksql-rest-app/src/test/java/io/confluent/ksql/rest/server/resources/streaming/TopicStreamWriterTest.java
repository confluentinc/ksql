package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.MatcherAssert.assertThat;

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
import java.util.Queue;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicStreamWriterTest {

  @Mock public KafkaConsumer<String, Bytes> kafkaConsumer;
  @Mock public SchemaRegistryClient schemaRegistry;

  @Before
  public void setup() {
    final Iterator<ConsumerRecords<String, Bytes>> records = IntStream.iterate(0, i -> i + 1)
        .mapToObj(i -> new ConsumerRecord<>(
            "topic", 0, i, String.valueOf(i), new Bytes(Ints.toByteArray(i))))
        .map(Lists::newArrayList)
        .map(crs -> (List<ConsumerRecord<String, Bytes>>) crs)
        .map(crs -> ImmutableMap.of(new TopicPartition("topic", 0), crs))
        .map(map -> (Map<TopicPartition, List<ConsumerRecord<String, Bytes>>>) map)
        .map(ConsumerRecords::new)
        .iterator();

    Mockito.when(kafkaConsumer.poll(Mockito.any(Duration.class)))
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
        "\\x00\\x00\\x00\\x00",
        "\\x00\\x00\\x00\\x01"
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
        "\\x00\\x00\\x00\\x00",
        "\\x00\\x00\\x00\\x02"
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

    void assertWrites(List<String> expected) {
      assertThat(recordedWrites.size(), Matchers.equalTo(expected.size()));
      for (int i = 0; i < recordedWrites.size(); i++) {
        final byte[] bytes = recordedWrites.get(i);
        assertThat(
            new String(bytes, Charsets.UTF_8),
            Matchers.containsString(expected.get(i)));
      }
    }
  }

}