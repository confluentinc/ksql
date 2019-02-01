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
    final TopicStreamWriter writer = new TopicStreamWriter(
        schemaRegistry,
        kafkaConsumer,
        "topic",
        1,
        Duration.ZERO,
        OptionalInt.of(2)
    );

    final List<String> expected = ImmutableList.of(
        "Format:STRING",
        "\\x00\\x00\\x00\\x00",
        "\\x00\\x00\\x00\\x01"
    );

    ValidatingOutputStream out = new ValidatingOutputStream(expected);
    writer.write(out);
    out.assertNoMore();
  }

  @Test
  public void testIntervalTwoAndLimitTwo() {
    final TopicStreamWriter writer = new TopicStreamWriter(
        schemaRegistry,
        kafkaConsumer,
        "topic",
        2,
        Duration.ZERO,
        OptionalInt.of(2)
    );

    final List<String> expected = ImmutableList.of(
        "Format:STRING",
        "\\x00\\x00\\x00\\x00",
        "\\x00\\x00\\x00\\x02"
    );

    ValidatingOutputStream out = new ValidatingOutputStream(expected);
    writer.write(out);
    out.assertNoMore();
  }

  private static class ValidatingOutputStream extends OutputStream {

    private final List<String> expected;
    private int numWrites = 0;

    ValidatingOutputStream(List<String> expected) {
      this.expected = expected;
    }

    @Override public void write(final int b) { /* not called*/ }

    @Override
    public void write(final byte[] b) {
      assertThat(numWrites, Matchers.lessThanOrEqualTo(expected.size()));
      assertThat(
          new String(b, Charsets.UTF_8),
          Matchers.containsString(expected.get(numWrites++)));
    }

    void assertNoMore() {
      assertThat(numWrites, Matchers.equalTo(expected.size() - 1));
    }
  }

}