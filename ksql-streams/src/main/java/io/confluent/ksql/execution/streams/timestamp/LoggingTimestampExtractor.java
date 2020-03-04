/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams.timestamp;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * A wrapper around {@code TimestampExtractor} that can be configured to suppress any
 * errors and instead returns a negative timestamp (indicating to streams that the message
 * should be ignored). Additionally, this class ensures that any errors are logged to the
 * processing log (even the fatal ones) for visibility.
 */
public class LoggingTimestampExtractor implements KsqlTimestampExtractor {

  private final KsqlTimestampExtractor delegate;
  private final ProcessingLogger logger;
  private final boolean failOnError;

  public LoggingTimestampExtractor(
      final KsqlTimestampExtractor delegate,
      final ProcessingLogger logger,
      final boolean failOnError
  ) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.logger = Objects.requireNonNull(logger, "logger");
    this.failOnError = failOnError;
  }

  @Override
  public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
    try {
      return delegate.extract(record, previousTimestamp);
    } catch (final Exception e) {
      logger.error(timestampExtractErroMsg(e, record.value().toString()));
      if (failOnError) {
        throw e;
      }
      return -1L;
    }
  }

  @Override
  public long extract(final GenericRow row) {
    try {
      return delegate.extract(row);
    } catch (final Exception e) {
      logger.error(timestampExtractErroMsg(e, row.toString()));
      if (failOnError) {
        throw e;
      }
      return -1L;
    }
  }

  @VisibleForTesting
  TimestampExtractor getDelegate() {
    return delegate;
  }

  public Function<ProcessingLogConfig, SchemaAndValue> timestampExtractErroMsg(
      final Exception e,
      final String row
  ) {
    return config -> {
      final Struct message = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
      final Struct error = new Struct(MessageType.RECORD_PROCESSING_ERROR.getSchema());

      error.put(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE, e.getMessage());

      if (config.getBoolean(ProcessingLogConfig.INCLUDE_ROWS)) {
        error.put(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD, row.toString());
      }

      final List<String> cause = ErrorMessageUtil.getErrorMessages(e);
      cause.remove(0);
      error.put(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_CAUSE, cause);

      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, message);
    };
  }
}
