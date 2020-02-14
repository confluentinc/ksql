package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.server.resources.streaming.PrintPublisher.PrintSubscription;
import io.confluent.ksql.rest.server.resources.streaming.StreamingTestUtils.TestSubscriber;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("UnstableApiUsage")
@RunWith(MockitoJUnitRunner.class)
public class PrintSubscriptionTest {

  @Mock public KafkaConsumer<Bytes, Bytes> kafkaConsumer;
  @Mock public SchemaRegistryClient schemaRegistry;

  @Before
  public void setup() {
    final Iterator<ConsumerRecords<Bytes, Bytes>> records = StreamingTestUtils.generate(
        "topic",
        i -> new Bytes(("key-" + i).getBytes(Charsets.UTF_8)),
        i -> new Bytes(("value-" + i).getBytes(Charsets.UTF_8)));

    final Iterator<ConsumerRecords<Bytes, Bytes>> partitioned =
        StreamingTestUtils.partition(records, 3);

    when(kafkaConsumer.poll(any(Duration.class)))
        .thenAnswer(invocation -> partitioned.next());
  }

  @Test
  public void testPrintPublisher() {
    // Given:
    final TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    final PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, null, null),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    final Collection<String> results = subscription.poll();

    // Then:
    assertThat(results, contains(Lists.newArrayList(
        containsString("key: key-0, value: value-0"),
        containsString("key: key-1, value: value-1"),
        containsString("key: key-2, value: value-2"))
    ));
  }

  @Test
  public void testPrintPublisherLimit() {
    // Given:
    final TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    final PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, null, 2),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    final Collection<String> results = subscription.poll();
    final Collection<String> results2 = subscription.poll();

    // Then:
    assertThat(results, contains(Lists.newArrayList(
        containsString("key-0, value: value-0"),
        containsString("key-1, value: value-1"))
    ));
    assertThat(results2, empty());
  }

  @Test
  public void testPrintPublisherLimitTwoBatches() {
    // Given:
    final TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    final PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, null, 5),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    final Collection<String> results = subscription.poll();
    final Collection<String> results2 = subscription.poll();

    // Then:
    assertThat(results, contains(Lists.newArrayList(
        containsString("key: key-0, value: value-0"),
        containsString("key: key-1, value: value-1"),
        containsString("key: key-2, value: value-2"))
    ));
    assertThat(results2, contains(Lists.newArrayList(
        containsString("key: key-3, value: value-3"),
        containsString("key: key-4, value: value-4"))
    ));
  }

  @Test
  public void testPrintPublisherIntervalNoLimit() {
    // Given:
    final TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    final PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, 2, null),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    final Collection<String> results = subscription.poll();
    final Collection<String> results2 = subscription.poll();

    // Then:
    assertThat(results, contains(Lists.newArrayList(
        containsString("key: key-0, value: value-0"),
        containsString("key: key-2, value: value-2"))
    ));
    assertThat(results2, contains(
        containsString("key: key-4, value: value-4")
    ));
  }

  @Test
  public void testPrintPublisherIntervalAndLimit() {
    // Given:
    final TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    final PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, 2, 4),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    final Collection<String> results = subscription.poll();
    final Collection<String> results2 = subscription.poll();
    final Collection<String> results3 = subscription.poll();
    final Collection<String> results4 = subscription.poll();

    // Then:
    assertThat(results, contains(Lists.newArrayList(
        containsString("key: key-0, value: value-0"),
        containsString("key: key-2, value: value-2"))
    ));
    assertThat(results2, contains(
        containsString("key: key-4, value: value-4")
    ));
    assertThat(results3, contains(
        containsString("key: key-6, value: value-6")
    ));
    assertThat(results4, empty());
  }
}

