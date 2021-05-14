/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.logging.processing.LoggingDeserializer;
import io.confluent.ksql.logging.processing.LoggingDeserializer.DelayedResult;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * The {@code StaticTopicSerde} hard codes the topic name that is passed
 * to the delegate Serde, regardless to what the caller passes in as the
 * topic. The only exception is that if a deserialization attempt fails,
 * the deserializer will attempt one more time using the topic that was
 * passed to the Serde (instead of the hard coded value). In this situation,
 * the {@code onFailure} callback is called so that the user of this class
 * can remedy the issue (i.e. register an extra schema under the hard coded
 * topic). The callback will not be called if both serialization attempts fail.
 *
 * <p>This class is intended as a workaround for the issues described in
 * both KAFKA-10179 and KSQL-5673; specifically, it allows a materialized
 * state store to use a different topic name than that which Kafka Streams
 * passes in to the Serde.</p>
 *
 * <p><b>Think carefully before reusing this class! It's inteded use case is
 * very narrow.</b></p>
 */
public final class StaticTopicSerde<T> implements Serde<T> {

  public interface Callback {

    /**
     * This method is called when the {@link Serde#deserializer()}'s produced by
     * this class' {@link Deserializer#deserialize(String, byte[])} method fails
     * using the static topic but succeeds using the source topic.
     *
     * @param sourceTopic the original topic that was passed in to the deserializer
     * @param staticTopic the hard coded topic that was passed into the {@code StaticTopicSerde}
     * @param data        the data that failed deserialization
     */
    void onDeserializationFailure(String sourceTopic, String staticTopic, byte[] data);
  }

  private final Serde<T> delegate;
  private final String topic;
  private final Callback onFailure;

  /**
   * @param topic     the topic to hardcode
   * @param serde     the delegate serde
   * @param onFailure a callback to call on failure
   *
   * @return a serde which delegates to {@code serde} but passes along {@code topic}
   *         in place of whatever the actual topic is
   */
  public static <S> Serde<S> wrap(
      final String topic,
      final Serde<S> serde,
      final Callback onFailure
  ) {
    return new StaticTopicSerde<>(topic, serde, onFailure);
  }

  private StaticTopicSerde(
      final String topic,
      final Serde<T> delegate,
      final Callback onFailure
  ) {
    this.topic = Objects.requireNonNull(topic, "topic");
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.onFailure = Objects.requireNonNull(onFailure, "onFailure");
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    delegate.configure(configs, isKey);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public Serializer<T> serializer() {
    final Serializer<T> serializer = delegate.serializer();
    return (topic, data) -> serializer.serialize(this.topic, data);
  }

  @Override
  public Deserializer<T> deserializer() {
    final Deserializer<T> deserializer = delegate.deserializer();

    if (deserializer instanceof LoggingDeserializer<?>) {
      final LoggingDeserializer<T> loggingDeserializer = (LoggingDeserializer<T>) deserializer;

      return (topic, data) -> {
        final DelayedResult<T> staticResult = loggingDeserializer.tryDeserialize(this.topic, data);
        if (!staticResult.isError()) {
          return staticResult.get();
        }

        // if both attempts error, then staticResult.get() will log the error to
        // the processing log and throw - do not call the callback in this case
        final DelayedResult<T> sourceResult = loggingDeserializer.tryDeserialize(topic, data);
        if (sourceResult.isError()) {
          return staticResult.get();
        }

        onFailure.onDeserializationFailure(topic, this.topic, data);
        return sourceResult.get();
      };
    }

    return (topic, data) -> {
      try {
        return deserializer.deserialize(this.topic, data);
      } catch (final Exception e) {
        final T object = deserializer.deserialize(topic, data);
        onFailure.onDeserializationFailure(topic, this.topic, data);
        return object;
      }
    };
  }

  @VisibleForTesting
  public String getTopic() {
    return topic;
  }

  @VisibleForTesting
  public Callback getOnFailure() {
    return onFailure;
  }
}
