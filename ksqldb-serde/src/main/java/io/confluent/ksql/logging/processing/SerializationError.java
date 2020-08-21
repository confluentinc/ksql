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

package io.confluent.ksql.logging.processing;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public class SerializationError<T> implements ProcessingLogger.ErrorMessage {

  private final Throwable exception;
  private final Optional<T> record;
  private final String topic;

  public SerializationError(
      final Throwable exception,
      final Optional<T> record,
      final String topic
  ) {
    this.exception = requireNonNull(exception, "exception");
    this.record = requireNonNull(record, "record");
    this.topic = requireNonNull(topic, "topic");
  }

  @Override
  public SchemaAndValue get(final ProcessingLogConfig config) {
    final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA)
        .put(ProcessingLogMessageSchema.TYPE, MessageType.SERIALIZATION_ERROR.getTypeId())
        .put(ProcessingLogMessageSchema.SERIALIZATION_ERROR, serializationError(config));

    return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SerializationError<?> that = (SerializationError) o;
    return Objects.equals(exception, that.exception)
        && Objects.equals(record, that.record)
        && Objects.equals(topic, that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(exception, record, topic);
  }

  private Struct serializationError(final ProcessingLogConfig config) {
    final Struct serializationError = new Struct(MessageType.SERIALIZATION_ERROR.getSchema())
        .put(
            ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_MESSAGE,
            exception.getMessage())
        .put(
            ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_CAUSE,
            getCause()
        )
        .put(
            ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_TOPIC,
            topic
        );

    if (config.getBoolean(ProcessingLogConfig.INCLUDE_ROWS)) {
      serializationError.put(
          ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_RECORD,
          record.map(Object::toString).orElse(null)
      );
    }

    return serializationError;
  }

  private List<String> getCause() {
    final List<String> cause = ErrorMessageUtil.getErrorMessages(exception);
    cause.remove(0);
    return cause;
  }
}
