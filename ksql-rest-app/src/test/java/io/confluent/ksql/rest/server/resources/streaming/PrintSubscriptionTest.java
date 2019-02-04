package io.confluent.ksql.rest.server.resources.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.MoreExecutors;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.rest.server.resources.streaming.PrintPublisher.PrintSubscription;
import io.confluent.ksql.rest.server.resources.streaming.StreamingTestUtils.TestSubscriber;
import io.confluent.ksql.rest.server.resources.streaming.TopicStream.RecordFormatter;
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

@RunWith(MockitoJUnitRunner.class)
public class PrintSubscriptionTest {

  @Mock public KafkaConsumer<String, Bytes> kafkaConsumer;
  @Mock public SchemaRegistryClient schemaRegistry;

  @Before
  public void setup() {
    final Iterator<ConsumerRecords<String, Bytes>> records = StreamingTestUtils.generate(
        "topic",
        i -> "key" + i,
        i -> new Bytes(("value" + i).getBytes(Charsets.UTF_8)));

    final Iterator<ConsumerRecords<String, Bytes>> partitioned =
        StreamingTestUtils.partition(records, 3);

    when(kafkaConsumer.poll(any(Duration.class)))
        .thenAnswer(invocation -> partitioned.next());
  }

  @Test
  public void testPrintPublisher() {
    // Given:
    TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, null, null),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    Collection<String> results = subscription.poll();

    // Then:
    assertThat(results, contains(
        containsString("key0 , value0"),
        containsString("key1 , value1"),
        containsString("key2 , value2")
    ));
  }

  @Test
  public void testPrintPublisherLimit() {
    // Given:
    TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, null, 2),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    Collection<String> results = subscription.poll();
    Collection<String> results2 = subscription.poll();

    // Then:
    assertThat(results, contains(
        containsString("key0 , value0"),
        containsString("key1 , value1")
    ));
    assertThat(results2, empty());
  }

  @Test
  public void testPrintPublisherLimitTwoBatches() {
    // Given:
    TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, null, 5),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    Collection<String> results = subscription.poll();
    Collection<String> results2 = subscription.poll();

    // Then:
    assertThat(results, contains(
        containsString("key0 , value0"),
        containsString("key1 , value1"),
        containsString("key2 , value2")
    ));
    assertThat(results2, contains(
        containsString("key3 , value3"),
        containsString("key4 , value4")
    ));
  }

  @Test
  public void testPrintPublisherIntervalNoLimit() {
    // Given:
    TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, 2, null),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    Collection<String> results = subscription.poll();
    Collection<String> results2 = subscription.poll();

    // Then:
    assertThat(results, contains(
        containsString("key0 , value0"),
        containsString("key2 , value2")
    ));
    assertThat(results2, contains(
        containsString("key4 , value4")
    ));
  }

  @Test
  public void testPrintPublisherIntervalAndLimit() {
    // Given:
    TestSubscriber<Collection<String>> subscriber = new TestSubscriber<>();
    PrintSubscription subscription = new PrintSubscription(
        MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1)),
        StreamingTestUtils.printTopic("topic", true, 2, 4),
        subscriber,
        kafkaConsumer,
        new RecordFormatter(schemaRegistry, "topic")
    );

    // When:
    Collection<String> results = subscription.poll();
    Collection<String> results2 = subscription.poll();
    Collection<String> results3 = subscription.poll();
    Collection<String> results4 = subscription.poll();

    // Then:
    assertThat(results, contains(
        containsString("key0 , value0"),
        containsString("key2 , value2")
    ));
    assertThat(results2, contains(
        containsString("key4 , value4")
    ));
    assertThat(results3, contains(
        containsString("key6 , value6")
    ));
    assertThat(results4, empty());
  }

}

