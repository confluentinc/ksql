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

package io.confluent.ksql.execution.streams.timestamp;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    this.delegate = requireNonNull(delegate, "delegate");
    this.logger = requireNonNull(logger, "logger");
    this.failOnError = failOnError;
  }

  @Override
  public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
    try {
      return delegate.extract(record, previousTimestamp);
    } catch (final RuntimeException e) {
      return handleFailure(record.key(), record.value(), e);
    }
  }

  @Override
  public long extract(final Object key, final GenericRow value) {
    try {
      return delegate.extract(key, value);
    } catch (final RuntimeException e) {
      return handleFailure(key, value, e);
    }
  }

  @VisibleForTesting
  TimestampExtractor getDelegate() {
    return delegate;
  }

  private long handleFailure(final Object key, final Object value, final RuntimeException e) {
    logger.error(RecordProcessingError.recordProcessingError(
        "Failed to extract timestamp from row",
        e,
        () -> "key:" + key + ", value:" + value
    ));

    if (failOnError) {
      throw e;
    }

    return -1L;
  }
}
