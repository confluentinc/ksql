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

package io.confluent.ksql.api.impl;

import static io.confluent.ksql.api.impl.KeyValueExtractor.convertColumnNameCase;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.api.util.ApiSqlValueCoercer;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.VertxUtils;
import io.vertx.core.Context;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InsertsSubscriber extends BaseSubscriber<JsonObject> implements
    InsertsStreamSubscriber {

  private static final Logger log = LoggerFactory.getLogger(InsertsSubscriber.class);
  private static final int REQUEST_BATCH_SIZE = 200;
  private static final SqlValueCoercer SQL_VALUE_COERCER = ApiSqlValueCoercer.INSTANCE;

  private final Producer<byte[], byte[]> producer;
  private final DataSource dataSource;
  private final Serializer<GenericKey> keySerializer;
  private final Serializer<GenericRow> valueSerializer;
  private final BufferedPublisher<InsertResult> acksPublisher;
  private final WorkerExecutor workerExecutor;
  private int outstandingTokens;
  private boolean drainHandlerSet;
  private long sequence;

  public static InsertsSubscriber createInsertsSubscriber(
      final ServiceContext serviceContext,
      final JsonObject properties,
      final DataSource dataSource,
      final KsqlConfig ksqlConfig,
      final Context context,
      final Subscriber<InsertResult> acksSubscriber,
      final WorkerExecutor workerExecutor
  ) {
    final KsqlConfig configCopy = ksqlConfig.cloneWithPropertyOverwrite(properties.getMap());
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(configCopy.originals());

    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getKsqlTopic().getKeyFormat().getFeatures(),
        dataSource.getKsqlTopic().getValueFormat().getFeatures()
    );

    final KeySerdeFactory keySerdeFactory = new GenericKeySerDe();
    final Serde<GenericKey> keySerde = keySerdeFactory.create(
        dataSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
        physicalSchema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );

    final ValueSerdeFactory valueSerdeFactory = new GenericRowSerDe();
    final Serde<GenericRow> valueSerde = valueSerdeFactory.create(
        dataSource.getKsqlTopic().getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE,
        Optional.empty()
    );

    final BufferedPublisher<InsertResult> acksPublisher = new BufferedPublisher<>(context);
    acksPublisher.subscribe(acksSubscriber);
    return new InsertsSubscriber(context, producer, dataSource, keySerde.serializer(),
        valueSerde.serializer(), acksPublisher, workerExecutor);
  }

  private InsertsSubscriber(
      final Context context,
      final Producer<byte[], byte[]> producer,
      final DataSource dataSource,
      final Serializer<GenericKey> keySerializer,
      final Serializer<GenericRow> valueSerializer,
      final BufferedPublisher<InsertResult> acksPublisher,
      final WorkerExecutor workerExecutor
  ) {
    super(context);
    this.producer = Objects.requireNonNull(producer);
    this.dataSource = Objects.requireNonNull(dataSource);
    this.keySerializer = Objects.requireNonNull(keySerializer);
    this.valueSerializer = Objects.requireNonNull(valueSerializer);
    this.acksPublisher = Objects.requireNonNull(acksPublisher);
    this.workerExecutor = Objects.requireNonNull(workerExecutor);
  }

  @Override
  public void close() {
    // Run async as it can block
    executeOnWorker(producer::close);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkRequest();
  }

  @Override
  protected void handleValue(final JsonObject jsonObjectWithCaseInsensitiveFields) {

    try {
      final JsonObject jsonObject = convertColumnNameCase(jsonObjectWithCaseInsensitiveFields);

      final GenericKey key = extractKey(jsonObject);
      final GenericRow values = extractValues(jsonObject);

      final String topicName = dataSource.getKafkaTopicName();
      final byte[] keyBytes = keySerializer.serialize(topicName, key);
      final byte[] valueBytes = valueSerializer.serialize(topicName, values);

      final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
          topicName,
          null,
          System.currentTimeMillis(),
          keyBytes,
          valueBytes
      );

      outstandingTokens--;
      producer.send(record, new SendCallback(sequence));
    } catch (Exception e) {
      // We send the error to the acks publisher
      acksPublisher.accept(InsertResult.failedInsert(sequence, e));
    }
    sequence++;
  }

  private void handleResult(final InsertResult result) {
    VertxUtils.checkContext(context);
    final boolean full = acksPublisher.accept(result);
    if (full) {
      if (!drainHandlerSet) {
        // Push back if the ack subscriber is slow
        acksPublisher.drainHandler(this::acksPublisherReceptive);
        drainHandlerSet = true;
      }
    } else {
      checkRequest();
    }
  }

  private void acksPublisherReceptive() {
    drainHandlerSet = false;
    checkRequest();
  }

  private void checkRequest() {
    if (outstandingTokens == 0) {
      outstandingTokens = REQUEST_BATCH_SIZE;
      makeRequest(REQUEST_BATCH_SIZE);
    }
  }

  private GenericKey extractKey(final JsonObject values) {
    return KeyValueExtractor.extractKey(values, dataSource.getSchema(), SQL_VALUE_COERCER);
  }

  private GenericRow extractValues(final JsonObject values) {
    return KeyValueExtractor.extractValues(values, dataSource.getSchema(), SQL_VALUE_COERCER);
  }

  private void executeOnWorker(final Runnable runnable) {
    workerExecutor.executeBlocking(p -> runnable.run(), false, ar -> {
      if (ar.failed()) {
        log.error("Failed to close producer", ar.cause());
      }
    });
  }

  private class SendCallback implements Callback {

    private final long seq;

    SendCallback(final long seq) {
      this.seq = seq;
    }

    @Override
    public void onCompletion(final RecordMetadata metadata, final Exception exception) {
      final InsertResult result = exception != null ? InsertResult.failedInsert(seq, exception) :
          InsertResult.succeededInsert(seq);
      context.runOnContext(v -> handleResult(result));
    }
  }

}
