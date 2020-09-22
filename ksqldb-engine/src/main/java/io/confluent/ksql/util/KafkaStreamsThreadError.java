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

package io.confluent.ksql.util;

import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Objects;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public final class KafkaStreamsThreadError implements ProcessingLogger.ErrorMessage {
  public static KafkaStreamsThreadError of(
      final String errorMsg,
      final Thread thread,
      final Throwable exception
  ) {
    return new KafkaStreamsThreadError(errorMsg, thread, exception);
  }

  private final String errorMsg;
  private final Thread thread;
  private final Throwable exception;

  private KafkaStreamsThreadError(
      final String errorMsg,
      final Thread thread,
      final Throwable exception
  ) {
    this.errorMsg = requireNonNull(errorMsg, "errorMsg");
    this.thread = requireNonNull(thread, "thread");
    this.exception = requireNonNull(exception, "exception");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KafkaStreamsThreadError that = (KafkaStreamsThreadError) o;
    return Objects.equals(errorMsg, that.errorMsg)
        && Objects.equals(thread.getName(), thread.getName())
        && Objects.equals(exception.getClass(), that.exception.getClass())
        && Objects.equals(exception.toString(), that.exception.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(thread, exception);
  }

  @Override
  public SchemaAndValue get(final ProcessingLogConfig config) {
    final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA)
        .put(ProcessingLogMessageSchema.TYPE,
            MessageType.KAFKA_STREAMS_THREAD_ERROR.getTypeId())
        .put(ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR,
            streamsThreadError());

    return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
  }

  private Struct streamsThreadError() {
    final Struct threadError = new Struct(MessageType.KAFKA_STREAMS_THREAD_ERROR.getSchema())
        .put(
            ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR_FIELD_NAME,
            thread.getName())
        .put(
            ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR_FIELD_MESSAGE,
            errorMsg)
        .put(
            ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR_FIELD_CAUSE,
            ErrorMessageUtil.getErrorMessages(exception));

    return threadError;
  }
}
