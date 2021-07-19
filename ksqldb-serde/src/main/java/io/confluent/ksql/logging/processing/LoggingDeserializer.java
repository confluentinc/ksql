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

package io.confluent.ksql.logging.processing;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;

public final class LoggingDeserializer<T> implements Deserializer<T> {

  private final Deserializer<T> delegate;
  private final ProcessingLogger processingLogger;
  private boolean isKey;

  public LoggingDeserializer(
      final Deserializer<T> delegate,
      final ProcessingLogger processingLogger
  ) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    this.isKey = isKey;
    delegate.configure(configs, isKey);
  }

  @Override
  public T deserialize(final String topic, final byte[] bytes) {
    return tryDeserialize(topic, bytes).get();
  }

  /**
   * Similar to {@link #deserialize(String, byte[])}, but allows the erroneous case
   * to delay the log-and-throw behavior until {@link DelayedResult#get()} is called.
   *
   * <p>This can be used in the scenarios when an error is expected, such as if a retry
   * will likely solve the problem, to avoid spamming the processing logger with messages
   * that are not helpful to the end user.</p>
   */
  public DelayedResult<T> tryDeserialize(final String topic, final byte[] bytes) {
    try {
      return new DelayedResult<T>(delegate.deserialize(topic, bytes));
    } catch (final RuntimeException e) {
      return new DelayedResult<T>(
          e,
          new DeserializationError(e, Optional.ofNullable(bytes), topic, isKey),
          processingLogger
      );
    }
  }

  @Override
  public void close() {
    delegate.close();
  }

  public static class DelayedResult<T> {

    private final T result;
    private final RuntimeException error;
    private final ProcessingLogger processingLogger;
    private final DeserializationError deserializationError;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public DelayedResult(
        final RuntimeException error,
        final DeserializationError deserializationError,
        final ProcessingLogger processingLogger
    ) {
      this.result = null;
      this.error = error;
      this.deserializationError = requireNonNull(deserializationError, "deserializationError");
      this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    }

    public DelayedResult(final T result) {
      this.result = result;
      this.error = null;
      this.deserializationError = null;
      this.processingLogger = null;
    }

    public boolean isError() {
      return error != null;
    }

    @VisibleForTesting
    RuntimeException getError() {
      return error;
    }

    @SuppressWarnings("ConstantConditions")
    public T get() {
      if (isError()) {
        processingLogger.error(deserializationError);
        throw error;
      }

      return result;
    }

  }

}
