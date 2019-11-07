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

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.server.computation.Command;
import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

/**
 * Produces to a Kafka topic in transactions. After initializing the producer, the general
 * transaction flow is to begin transaction, send any number of records to command topic,
 * and commit/abort the transaction. Calling methods in an out of order manner will result in a
 * {@link KafkaException}.
 */
@NotThreadSafe
public interface TransactionalProducer extends Closeable {
  
  /**
   * Initializes the transactional producer. This should be called only once before any other
   * method. Calling this will 1. Ensure transactions initiated by previous instances of the
   * producer with the same transactional id are completed. If the previous transaction
   * has begun completion, this awaits until it's finished. Otherwise, a previous instance of the
   * transactional producer with the same id will be fenced off and unable to move forward with
   * its transaction 2. Get the internal producer id and epoch which is used in future transactional
   * messages issued by this producer.
   */
  void initialize();

  /**
   * Begins a transaction session.
   * @throws KafkaException if initialize() not called or an active transaction is in progress
   */
  void begin();

  /**
   * Sends a command record to the topic. The record will be present in the command topic,
   * but transactional consumers won't be able to read the record until it's been
   * successfully committed. Multiple send() can be called in a single transaction.
   * @throws KafkaException if no active transaction session is in progress
   */
  RecordMetadata send(CommandId commandId, Command command);

  /**
   * Aborts the current transaction. Records that are part of the current transaction won't be
   * read by transactional consumers.
   * @throws KafkaException if no active transaction session is in progress
   */
  void abort();

  /**
   * Commits the current transaction. After successfully committing a transaction,
   * transactional consumers will be able to read the records from the topic.
   * @throws KafkaException if no active transaction session is in progress
   */
  void commit();

  /**
   * Closes the transactional producer so that no more writes will be accepted. Aborts
   * the current transaction session if present.
   */
  void close() throws IOException;
}
