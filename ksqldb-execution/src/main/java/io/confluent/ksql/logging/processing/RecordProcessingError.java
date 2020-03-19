/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For errors generated while processing a row.
 */
public final class RecordProcessingError implements ProcessingLogger.ErrorMessage {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessingError.class);

  public static ErrorMessage recordProcessingError(
      final String errorMsg, final Throwable exception, final GenericRow record
  ) {
    return new RecordProcessingError(
        errorMsg,
        Optional.of(exception),
        Optional.ofNullable(record).map(r -> () -> serializeRow(r))
    );
  }

  public static ErrorMessage recordProcessingError(
      final String errorMsg, final GenericRow record
  ) {
    return new RecordProcessingError(
        errorMsg,
        Optional.empty(),
        Optional.ofNullable(record).map(r -> () -> serializeRow(r))
    );
  }

  public static ErrorMessage recordProcessingError(
      final String errorMsg, final Throwable exception, final Supplier<String> record
  ) {
    return new RecordProcessingError(
        errorMsg,
        Optional.of(exception),
        Optional.ofNullable(record)
    );
  }

  private final String errorMsg;
  private final Optional<Throwable> exception;
  private final Optional<Supplier<String>> record;

  private RecordProcessingError(
      final String errorMsg,
      final Optional<Throwable> exception,
      final Optional<Supplier<String>> record
  ) {
    this.errorMsg = requireNonNull(errorMsg, "errorMsg");
    this.exception = requireNonNull(exception, "exception");
    this.record = requireNonNull(record, "record");
  }

  public String getMessage() {
    return errorMsg;
  }

  @Override
  public SchemaAndValue get(final ProcessingLogConfig config) {
    final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
    struct.put(ProcessingLogMessageSchema.TYPE, MessageType.RECORD_PROCESSING_ERROR.getTypeId());
    struct.put(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR, processingError(config));

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
    final RecordProcessingError that = (RecordProcessingError) o;
    return Objects.equals(errorMsg, that.errorMsg)
        && Objects.equals(exception, that.exception)
        && Objects.equals(record.map(Supplier::get), that.record.map(Supplier::get));
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorMsg, exception, record);
  }

  private Struct processingError(final ProcessingLogConfig config) {
    final Struct recordProcessingError =
        new Struct(MessageType.RECORD_PROCESSING_ERROR.getSchema());

    recordProcessingError.put(
        ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE,
        errorMsg
    );

    recordProcessingError.put(
        ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_CAUSE,
        exception.map(RecordProcessingError::getCause)
            .orElse(Collections.emptyList())
    );

    if (config.getBoolean(ProcessingLogConfig.INCLUDE_ROWS)) {
      record.ifPresent(r -> recordProcessingError.put(
          ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD,
          r.get()
      ));
    }

    return recordProcessingError;
  }

  private static List<String> getCause(final Throwable e) {
    final List<String> cause = ErrorMessageUtil.getErrorMessages(e);
    cause.remove(0);
    return cause;
  }

  private static String serializeRow(final GenericRow record) {
    try {
      return JsonMapper.INSTANCE.mapper.writeValueAsString(record.values());
    } catch (final Throwable t) {
      LOGGER.error("error serializing record for processing log", t);
      return null;
    }
  }
}
