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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;

public final class LoggingDeserializer implements Deserializer<GenericRow> {

  private final Deserializer<GenericRow> delegate;
  private final ProcessingLogger processingLogger;

  public LoggingDeserializer(
      final Deserializer<GenericRow> delegate,
      final ProcessingLogger processingLogger
  ) {
    this.delegate = requireNonNull(delegate, "delegate");
    this.processingLogger = requireNonNull(processingLogger, "processingLogger");
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    delegate.configure(configs, isKey);
  }

  @Override
  public GenericRow deserialize(final String topic, final byte[] bytes) {
    try {
      return delegate.deserialize(topic, bytes);
    } catch (final RuntimeException e) {
      processingLogger.error(
          SerdeProcessingLogMessageFactory.deserializationErrorMsg(e, Optional.ofNullable(bytes))
      );
      throw e;
    }
  }

  @Override
  public void close() {
    delegate.close();
  }
}
