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

package io.confluent.ksql.rest.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.Command;
import io.confluent.ksql.rest.server.computation.CommandRunner;
import io.confluent.ksql.rest.server.computation.CommandStore;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.util.InternalTopicJsonSerdeUtil;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Produces to the command topic in transactions. After initializing the producer, the general
 * transaction flow is to begin transaction, send any number of records to command topic,
 * and commit/abort the transaction. Calling methods in an out of order manner will result in a
 * {@link KafkaException}.
 * Ensures that commands are only produced to the command topic if the server is
 * in sync (commandRunner has processed all available records in the command topic).
 * Consists of a transactional producer and consumer.
 */
@NotThreadSafe
public class TransactionalProducer implements Producer<CommandId, Command> {


  private static final Logger LOG = LoggerFactory.getLogger(TransactionalProducer.class);
  
  private static final int COMMAND_TOPIC_PARTITION = 0;
  private final TopicPartition commandTopicPartition;
  private final String commandTopicName;
  
  private final Consumer<CommandId, Command> commandConsumer;
  private final Producer<CommandId, Command> commandProducer;
  private final Duration commandQueueCatchupTimeout;
  private final CommandStore commandStore;
  private final Closer closer;

  public TransactionalProducer(
      final String commandTopicName,
      final CommandStore commandStore,
      final Duration commandQueueCatchupTimeout,
      final Map<String, Object> kafkaConsumerProperties,
      final Map<String, Object> kafkaProducerProperties
  ) {
    this(
        commandTopicName,
        commandStore,
        commandQueueCatchupTimeout,
        new KafkaConsumer<>(
          Objects.requireNonNull(kafkaConsumerProperties, "kafkaConsumerProperties"),
          InternalTopicJsonSerdeUtil.getJsonDeserializer(CommandId.class, true),
          InternalTopicJsonSerdeUtil.getJsonDeserializer(Command.class, false)
        ),
        new KafkaProducer<>(
          Objects.requireNonNull(kafkaProducerProperties, "kafkaProducerProperties"),
          InternalTopicJsonSerdeUtil.getJsonSerializer(true),
          InternalTopicJsonSerdeUtil.getJsonSerializer(false)
        )
    );
  }

  @VisibleForTesting
  TransactionalProducer(
      final String commandTopicName,
      final CommandStore commandRunner,
      final Duration commandQueueCatchupTimeout,
      final Consumer<CommandId, Command> commandConsumer,
      final Producer<CommandId, Command> commandProducer
  ) {
    this.commandTopicPartition = new TopicPartition(
        Objects.requireNonNull(commandTopicName, "commandTopicName"),
        COMMAND_TOPIC_PARTITION
    );
    this.commandQueueCatchupTimeout = 
        Objects.requireNonNull(commandQueueCatchupTimeout, "commandQueueCatchupTimeout");
    this.commandConsumer = Objects.requireNonNull(commandConsumer, "commandConsumer");
    this.commandProducer = Objects.requireNonNull(commandProducer, "commandProducer");
    this.commandTopicName = Objects.requireNonNull(commandTopicName, "commandTopicName");
    this.commandStore = Objects.requireNonNull(commandRunner, "commandRunner");
    this.closer = Closer.create();
    closer.register(commandConsumer);
    closer.register(commandProducer);
  }

  /**
   * Initializes the transactional producer. This should be called only once before any other
   * method. Calling this will 
   * 1. Ensure transactions initiated by previous instances of the
   * producer with the same transactional id are completed. If the previous transaction
   * has begun completion, this awaits until it's finished. Otherwise, a previous instance of the
   * transactional producer with the same id will be fenced off and unable to move forward with
   * its transaction (begin, send, commit, abort, will throw a ProducerFencedException)
   * 2. Get the internal producer id and epoch which is used in future transactional
   * messages issued by this producer.
   */
  public void initTransactions() {
    commandProducer.initTransactions();
    commandConsumer.assign(Collections.singleton(commandTopicPartition));
  }

  /**
   * Begins a transaction session and waits for the {@link CommandRunner} to process all
   * available records in the command topic
   * @throws KafkaException if initialize() not called or an active transaction is in progress
   */
  public void beginTransaction() {
    commandProducer.beginTransaction();
    waitForConsumer();
  }
  
  /**
   * Helper function that blocks until the commandStore has consumed past the current
   * end offset of the command topic and the commandRunner has processed those records.
   */
  private void waitForConsumer() {
    try {
      final long endOffset = getEndOffset();

      commandStore.ensureConsumedPast(endOffset - 1, commandQueueCatchupTimeout);
    } catch (final InterruptedException e) {
      final String errorMsg = 
          "Interrupted while waiting for commandRunner to process command topic";
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, errorMsg, new KsqlEntityList()));
    } catch (final TimeoutException e) {
      final String errorMsg =
          "Timeout while waiting for commandRunner to process command topic";
      throw new KsqlRestException(
          Errors.serverErrorForStatement(e, errorMsg, new KsqlEntityList()));
    }
  }
  
  private long getEndOffset() {
    return commandConsumer.endOffsets(Collections.singletonList(commandTopicPartition))
        .get(commandTopicPartition);
  }

  /**
   * Aborts the current transaction. Records that are part of the current transaction won't be
   * read by transactional consumers.
   * @throws KafkaException if no active transaction session is in progress
   */
  public void abortTransaction() {
    commandProducer.abortTransaction();
  }

  /**
   * Commits the current transaction. After successfully committing a transaction,
   * transactional consumers will be able to read the records from the topic.
   * @throws KafkaException if no active transaction session is in progress
   */
  public void commitTransaction() {
    commandProducer.commitTransaction();
  }

  /**
   * Closes the transactional producer so that no more writes will be accepted. Aborts
   * the current transaction session if present.
   */
  public void close() {
    try {
      closer.close();
    } catch (IOException e) {
      LOG.error("Failed to close TransactionalProducer\n"
          + "Error: " + e.getMessage());
    }
  }

  // Not used currently
  public void close(final Duration duration) {
    throw new NotImplementedException(
        "TransactionalProducer.close(Duration duration) not implemented.");
  }

  /**
   * Sends a command record to the topic. The record will be present in the command topic,
   * but transactional consumers won't be able to read the record until it's been
   * successfully committed. Multiple send() can be called in a single transaction.
   * @throws KafkaException if no active transaction session is in progress
   */
  public RecordMetadata sendRecord(
      final CommandId commandId,
      final Command command
  ) {
    final ProducerRecord<CommandId, Command> producerRecord = new ProducerRecord<>(
        commandTopicName,
        COMMAND_TOPIC_PARTITION,
        Objects.requireNonNull(commandId, "commandId"),
        Objects.requireNonNull(command, "command"));
    try {
      return send(producerRecord).get();
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KafkaException e) {
      throw new KafkaException(e);
    }
  }
  
  public Future<RecordMetadata> send(
      final ProducerRecord<CommandId, Command> producerRecord
  ) {
    return commandProducer.send(producerRecord);
  }
  
  // These functions aren't used currently
  public Future<RecordMetadata> send(
      final ProducerRecord<CommandId, Command> producerRecord,
      final Callback callback
  ) {
    return commandProducer.send(producerRecord, callback);
  }

  public void sendOffsetsToTransaction(
      final Map<TopicPartition, OffsetAndMetadata> offsetsMap,
      final String consumerGroupId
  ) throws ProducerFencedException {
    commandProducer.sendOffsetsToTransaction(offsetsMap, consumerGroupId);
  }

  public void flush() {
    commandProducer.flush();
  }

  public List<PartitionInfo> partitionsFor(final String partitionName) {
    return commandProducer.partitionsFor(partitionName);
  }

  public Map<MetricName, ? extends Metric> metrics() {
    return commandProducer.metrics();
  }
}
