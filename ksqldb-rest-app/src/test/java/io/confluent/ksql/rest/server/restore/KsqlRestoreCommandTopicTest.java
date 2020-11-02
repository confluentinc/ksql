/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.restore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlRestoreCommandTopicTest {
  private static final String COMMAND_TOPIC_NAME = "command_topic_name";

  private static final int INTERNAL_TOPIC_PARTITION_COUNT = 1;
  private static final short INTERNAL_TOPIC_REPLICAS_COUNT = 1;

  private static final ImmutableMap<String, ?> INTERNAL_TOPIC_CONFIG = ImmutableMap.of(
      TopicConfig.RETENTION_MS_CONFIG, -1L,
      TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
      TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, false,
      TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, INTERNAL_TOPIC_REPLICAS_COUNT
  );

  private static final Pair<byte[], byte[]> COMMAND_1 = createStreamCommand("stream1");
  private static final Pair<byte[], byte[]> COMMAND_2 = createStreamCommand("stream2");
  private static final Pair<byte[], byte[]> COMMAND_3 = createStreamCommand("stream3");

  private static final ProducerRecord<byte[], byte[]> RECORD_1 = newRecord(COMMAND_1);
  private static final ProducerRecord<byte[], byte[]> RECORD_2 = newRecord(COMMAND_2);
  private static final ProducerRecord<byte[], byte[]> RECORD_3 = newRecord(COMMAND_3);

  private static final List<Pair<byte[], byte[]>> BACKUP_COMMANDS =
      Arrays.asList(COMMAND_1, COMMAND_2, COMMAND_3);

  private static Pair<byte[], byte[]> createStreamCommand(final String streamName) {
    return Pair.of(
        String.format("\"stream/%s/create\"", streamName).getBytes(StandardCharsets.UTF_8),
        String.format("{\"statement\":\"CREATE STREAM %s (id INT) WITH (kafka_topic='%s')\","
                + "\"streamsProperties\":{},\"originalProperties\":{},\"plan\":null}",
            streamName, streamName).getBytes(StandardCharsets.UTF_8)
    );
  }

  private static ProducerRecord<byte[], byte[]> newRecord(final Pair<byte[], byte[]> command) {
    return new ProducerRecord<>(
        COMMAND_TOPIC_NAME,
        0,
        command.getLeft(),
        command.getRight());
  }

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private Producer<byte[], byte[]> kafkaProducer;
  @Mock
  private Future<RecordMetadata> future1;
  @Mock
  private Future<RecordMetadata> future2;
  @Mock
  private Future<RecordMetadata> future3;

  private KsqlRestoreCommandTopic restoreCommandTopic;


  @Before
  public void setup() {
    final KsqlConfig serverConfig = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY, INTERNAL_TOPIC_REPLICAS_COUNT,
        KsqlConfig.KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY, INTERNAL_TOPIC_REPLICAS_COUNT
    ));

    restoreCommandTopic = new KsqlRestoreCommandTopic(
        serverConfig,
        COMMAND_TOPIC_NAME,
        topicClient,
        () -> kafkaProducer
    );

    when(kafkaProducer.send(RECORD_1)).thenReturn(future1);
    when(kafkaProducer.send(RECORD_2)).thenReturn(future2);
    when(kafkaProducer.send(RECORD_3)).thenReturn(future3);
  }

  @Test
  public void shouldCreateAndRestoreCommandTopic() throws ExecutionException, InterruptedException {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(false);

    // When:
    restoreCommandTopic.restore(BACKUP_COMMANDS);

    // Then:
    verifyCreateTopic(COMMAND_TOPIC_NAME);

    final InOrder inOrder = inOrder(kafkaProducer, future1, future2, future3);
    inOrder.verify(kafkaProducer).initTransactions();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_1);
    inOrder.verify(future1).get();
    inOrder.verify(kafkaProducer).commitTransaction();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_2);
    inOrder.verify(future2).get();
    inOrder.verify(kafkaProducer).commitTransaction();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_3);
    inOrder.verify(future3).get();
    inOrder.verify(kafkaProducer).commitTransaction();
    inOrder.verify(kafkaProducer).close();
    verifyNoMoreInteractions(kafkaProducer, future1, future2, future3);
  }

  @Test
  public void shouldThrowWhenRestoreIsInterrupted() throws Exception {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(false);
    doThrow(new InterruptedException("fail")).when(future2).get();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> restoreCommandTopic.restore(BACKUP_COMMANDS));

    // Then:
    assertThat(e.getMessage(), containsString("Restore process was interrupted."));
    verifyCreateTopic(COMMAND_TOPIC_NAME);
    final InOrder inOrder = inOrder(kafkaProducer, future1, future2);
    inOrder.verify(kafkaProducer).initTransactions();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_1);
    inOrder.verify(future1).get();
    inOrder.verify(kafkaProducer).commitTransaction();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_2);
    inOrder.verify(future2).get();
    inOrder.verify(kafkaProducer).abortTransaction();
    inOrder.verify(kafkaProducer).close();
    verifyNoMoreInteractions(kafkaProducer, future1, future2);
    verifyZeroInteractions(future3);
  }

  @Test
  public void shouldThrowWhenRestoreExecutionFails() throws Exception {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(false);
    doThrow(new RuntimeException()).when(future2).get();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> restoreCommandTopic.restore(BACKUP_COMMANDS));

    // Then:
    assertThat(e.getMessage(),
        containsString(String.format("Failed restoring command (line 2): %s",
            new String(RECORD_2.key(), StandardCharsets.UTF_8))));

    verifyCreateTopic(COMMAND_TOPIC_NAME);
    final InOrder inOrder = inOrder(kafkaProducer, future1, future2);
    inOrder.verify(kafkaProducer).initTransactions();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_1);
    inOrder.verify(future1).get();
    inOrder.verify(kafkaProducer).commitTransaction();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_2);
    inOrder.verify(future2).get();
    inOrder.verify(kafkaProducer).abortTransaction();
    inOrder.verify(kafkaProducer).close();
    verifyNoMoreInteractions(kafkaProducer, future1, future2);
    verifyZeroInteractions(future3);
  }

  @Test
  public void shouldRestoreCommandTopicWithEmptyCommands() {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(false);

    // When:
    restoreCommandTopic.restore(Collections.emptyList());

    // Then:
    verifyCreateTopic(COMMAND_TOPIC_NAME);
    final InOrder inOrder = inOrder(kafkaProducer);
    inOrder.verify(kafkaProducer).initTransactions();
    inOrder.verify(kafkaProducer).close();
    verifyZeroInteractions(kafkaProducer, future1, future2, future3);
  }

  @Test
  public void shouldDeleteAndCreateCommandTopicOnRestore() throws Exception {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(true).thenReturn(false);

    // When:
    restoreCommandTopic.restore(Collections.singletonList(BACKUP_COMMANDS.get(0)));

    // Then:
    verifyDeleteTopic(COMMAND_TOPIC_NAME);
    verifyCreateTopic(COMMAND_TOPIC_NAME);
    final InOrder inOrder = inOrder(kafkaProducer, future1);
    inOrder.verify(kafkaProducer).initTransactions();
    inOrder.verify(kafkaProducer).beginTransaction();
    inOrder.verify(kafkaProducer).send(RECORD_1);
    inOrder.verify(future1).get();
    inOrder.verify(kafkaProducer).commitTransaction();
    inOrder.verify(kafkaProducer).close();
    verifyNoMoreInteractions(kafkaProducer, future1);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldThrowIfCannotDescribeTopicExists() {
    // Given:
    doThrow(new RuntimeException("denied")).when(topicClient).isTopicExists(COMMAND_TOPIC_NAME);

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> restoreCommandTopic.restore(Collections.singletonList(BACKUP_COMMANDS.get(0))));

    // Then:
    assertThat(e.getMessage(), containsString("denied"));
    verifyZeroInteractions(kafkaProducer);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldThrowIfCannotDeleteTopic() {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(true).thenReturn(true);
    doThrow(new RuntimeException("denied")).when(topicClient)
        .deleteTopics(Collections.singletonList(COMMAND_TOPIC_NAME));

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> restoreCommandTopic.restore(Collections.singletonList(BACKUP_COMMANDS.get(0))));

    // Then:
    assertThat(e.getMessage(), containsString("denied"));
    verify(topicClient).isTopicExists(COMMAND_TOPIC_NAME);
    verify(topicClient).deleteTopics(Collections.singletonList(COMMAND_TOPIC_NAME));
    verifyNoMoreInteractions(topicClient);
    verifyZeroInteractions(kafkaProducer);
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldThrowIfCannotCreateTopic() {
    // Given:
    when(topicClient.isTopicExists(COMMAND_TOPIC_NAME)).thenReturn(false);
    doThrow(new RuntimeException("denied")).when(topicClient)
        .createTopic(COMMAND_TOPIC_NAME, INTERNAL_TOPIC_PARTITION_COUNT,
            INTERNAL_TOPIC_REPLICAS_COUNT, INTERNAL_TOPIC_CONFIG);

    // When:
    final Exception e = assertThrows(
        RuntimeException.class,
        () -> restoreCommandTopic.restore(Collections.singletonList(BACKUP_COMMANDS.get(0))));

    // Then:
    assertThat(e.getMessage(), containsString("denied"));
    verify(topicClient, times(2)).isTopicExists(COMMAND_TOPIC_NAME);
    verifyCreateTopic(COMMAND_TOPIC_NAME);
    verifyNoMoreInteractions(topicClient);
    verifyZeroInteractions(kafkaProducer);
  }

  private void verifyDeleteTopic(final String topicName) {
    verify(topicClient).deleteTopics(Collections.singletonList(topicName));
  }

  private void verifyCreateTopic(final String topicName) {
    verify(topicClient).createTopic(
        topicName,
        INTERNAL_TOPIC_PARTITION_COUNT,
        INTERNAL_TOPIC_REPLICAS_COUNT,
        INTERNAL_TOPIC_CONFIG);
  }
}
