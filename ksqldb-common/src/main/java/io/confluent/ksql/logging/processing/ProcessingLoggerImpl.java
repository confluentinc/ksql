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

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.logging.log4j.Logger;

public class ProcessingLoggerImpl implements ProcessingLogger {

  private final Logger inner;
  private final ProcessingLogConfig config;

  public ProcessingLoggerImpl(final ProcessingLogConfig config, final Logger innerLogger) {
    this.config = requireNonNull(config, "config");
    this.inner = requireNonNull(innerLogger, "inner");
  }

  @Override
  public void error(final ErrorMessage msg) {
    inner.error(() -> throwIfNotRightSchema(msg.get(config)));
  }

  @Override
  public void close() {
    // no-op for now
  }

  private static SchemaAndValue throwIfNotRightSchema(final SchemaAndValue schemaAndValue) {
    if (!schemaAndValue.schema().equals(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA)) {
      throw new RuntimeException("Received message with invalid schema");
    }

    return schemaAndValue;
  }
}