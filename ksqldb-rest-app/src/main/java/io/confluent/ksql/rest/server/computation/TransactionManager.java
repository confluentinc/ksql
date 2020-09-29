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

package io.confluent.ksql.rest.server.computation;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.Errors;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * This class is used for executing transactions when producing records to Kafka. TransactionManager
 * creates a new instance of {@link KafkaProducer} for each transaction.
 * </p>
 * The TransactionManager is built for a key/value serializer only.
 *
 * @param <K> The key serializer to use when creating instances of KafkaProducer
 * @param <V> The value serializer to use when creating instances of KafkaProducer
 */
public final class TransactionManager<K, V> {
  private static final String ACKS_WAIT_FOR_ALL_REPLICAS = "all";

  private final Map<String, Object> producerProperties;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Errors errorMessageHandler;
  private final KafkaProducerSupplier<K, V> kafkaProducerSupplier;

  interface KafkaProducerSupplier<K, V> {
    KafkaProducer<K, V> newProducer(
        Map<String, Object> properties,
        Serializer<K> keySerializer,
        Serializer<V> valueSerializer
    );
  }

  public TransactionManager(
      final String transactionalId,
      final Map<String, Object> producerProperties,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer,
      final Errors errorMessageHandler
  ) {
    this(
        transactionalId,
        producerProperties,
        keySerializer,
        valueSerializer,
        errorMessageHandler,
        KafkaProducer::new
    );
  }

  @VisibleForTesting
  TransactionManager(
      final String transactionalId,
      final Map<String, Object> producerProperties,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer,
      final Errors errorMessageHandler,
      final KafkaProducerSupplier<K, V> kafkaProducerSupplier
  ) {
    this.producerProperties = copyAndSetTransactionConfigs(
        transactionalId,
        requireNonNull(producerProperties, "producerProperties")
    );

    this.keySerializer = requireNonNull(keySerializer, "keySerializer");
    this.valueSerializer = requireNonNull(valueSerializer, "valueSerializer");
    this.errorMessageHandler = requireNonNull(errorMessageHandler, "errorMessageHandler");
    this.kafkaProducerSupplier = requireNonNull(kafkaProducerSupplier, "kafkaProducerSupplier");
  }

  private Map<String, Object> copyAndSetTransactionConfigs(
      final String transactionalId,
      final Map<String, Object> producerProperties
  ) {
    final ImmutableMap.Builder<String, Object> mapBuilder = new ImmutableMap.Builder<>();

    mapBuilder.putAll(producerProperties);
    mapBuilder.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    mapBuilder.put(ProducerConfig.ACKS_CONFIG, ACKS_WAIT_FOR_ALL_REPLICAS);

    return mapBuilder.build();
  }

  /**
   * Executes the {@code transactionBlock} as a single transaction.
   *
   * @param transactionBlock The block of code to execute in a single transaction.*
   */
  public <T> T executeTransaction(final Function<Producer<K, V>, T> transactionBlock) {
    try (Producer<K, V> producer = createTransactionalProducer()) {
      producer.beginTransaction();

      try {
        final T result = transactionBlock.apply(producer);
        producer.commitTransaction();
        return result;
      } catch (final ProducerFencedException
          | OutOfOrderSequenceException
          | AuthorizationException e
      ) {
        // We can't recover from these exceptions, so our only option is close producer and exit.
        // This catch doesn't abort() since doing that would throw another exception.
        throw e;
      } catch (final Exception e) {
        producer.abortTransaction();
        throw e;
      }
    }
  }

  private Producer<K, V> createTransactionalProducer() {
    final Producer<K, V> producer = kafkaProducerSupplier
        .newProducer(producerProperties, keySerializer, valueSerializer);

    try {
      producer.initTransactions();
    } catch (final TimeoutException e) {
      throw new RuntimeException(errorMessageHandler.transactionInitTimeoutErrorMessage(e), e);
    }

    return producer;
  }
}
