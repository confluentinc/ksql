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

package io.confluent.ksql.api.plugin;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.impl.Utils;
import io.confluent.ksql.api.server.BaseSubscriber;
import io.confluent.ksql.api.server.BufferedPublisher;
import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.logging.processing.NoopProcessingLogContext;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.schema.ksql.DefaultSqlValueCoercer;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.serde.GenericKeySerDe;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeySerdeFactory;
import io.confluent.ksql.serde.ValueSerdeFactory;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Struct;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertsSubscriber extends BaseSubscriber<JsonObject> {

  private static final Logger log = LoggerFactory.getLogger(InsertsSubscriber.class);
  private static final int REQUEST_BATCH_SIZE = 1000;
  private static final SqlValueCoercer SQL_VALUE_COERCER = DefaultSqlValueCoercer.INSTANCE;

  private final Producer<byte[], byte[]> producer;
  private final DataSource dataSource;
  private final Serializer<Struct> keySerializer;
  private final Serializer<GenericRow> valueSerializer;
  private final BufferedPublisher<InsertResult> acksPublisher;
  private int outstandingTokens;
  private boolean drainHandlerSet;
  private long seq;

  public static InsertsSubscriber createInsertsSubscriber(final ServiceContext serviceContext,
      final JsonObject properties, final DataSource dataSource, final KsqlConfig ksqlConfig,
      final Context context, final Subscriber<InsertResult> acksSubscriber) {
    final KsqlConfig configCopy = ksqlConfig.cloneWithPropertyOverwrite(properties.getMap());
    final Producer<byte[], byte[]> producer = serviceContext
        .getKafkaClientSupplier()
        .getProducer(configCopy.originals());

    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        dataSource.getSchema(),
        dataSource.getSerdeOptions()
    );

    final KeySerdeFactory keySerdeFactory = new GenericKeySerDe();
    final Serde<Struct> keySerde = keySerdeFactory.create(
        dataSource.getKsqlTopic().getKeyFormat().getFormatInfo(),
        physicalSchema.keySchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );

    final ValueSerdeFactory valueSerdeFactory = new GenericRowSerDe();
    final Serde<GenericRow> valueSerde = valueSerdeFactory.create(
        dataSource.getKsqlTopic().getValueFormat().getFormatInfo(),
        physicalSchema.valueSchema(),
        ksqlConfig,
        serviceContext.getSchemaRegistryClientFactory(),
        "",
        NoopProcessingLogContext.INSTANCE
    );

    final BufferedPublisher<InsertResult> acksPublisher = new BufferedPublisher<>(context);
    acksPublisher.subscribe(acksSubscriber);
    return new InsertsSubscriber(context, producer, dataSource, keySerde.serializer(),
        valueSerde.serializer(), acksPublisher);
  }

  protected InsertsSubscriber(final Context context,
      final Producer<byte[], byte[]> producer, final DataSource dataSource,
      final Serializer<Struct> keySerializer,
      final Serializer<GenericRow> valueSerializer,
      final BufferedPublisher<InsertResult> acksPublisher) {
    super(context);
    this.producer = Objects.requireNonNull(producer);
    this.dataSource = Objects.requireNonNull(dataSource);
    this.keySerializer = Objects.requireNonNull(keySerializer);
    this.valueSerializer = Objects.requireNonNull(valueSerializer);
    this.acksPublisher = Objects.requireNonNull(acksPublisher);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    checkRequest();
  }

  @Override
  protected void handleValue(final JsonObject jsonObject) {

    try {
      final Struct key = extractKey(jsonObject);
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
      producer.send(record, new SendCallback(seq));
    } catch (Exception e) {
      // We send the error to the acks publisher
      acksPublisher.accept(InsertResult.failedInsert(seq, e));
    }
    seq++;
  }

  private void handleResult(final InsertResult result) {
    Utils.checkContext(context);
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

  private Struct extractKey(final JsonObject values) {
    return KeyValueExtractor.extractKey(values, dataSource.getSchema(), SQL_VALUE_COERCER);
  }

  private GenericRow extractValues(final JsonObject values) {
    return KeyValueExtractor.extractValues(values, dataSource.getSchema(), SQL_VALUE_COERCER);
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
