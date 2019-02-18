/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.logging.processing;

import io.confluent.common.logging.StructuredLogger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.SchemaAndValue;

public class ProcessingLoggerImpl implements ProcessingLogger {
  private final StructuredLogger inner;
  private final ProcessingLogConfig config;

  private static class ProcessingLogMessage implements Supplier<SchemaAndValue> {
    final ProcessingLogConfig config;
    final Function<ProcessingLogConfig, SchemaAndValue> msgFactory;

    ProcessingLogMessage(
        final ProcessingLogConfig config,
        final Function<ProcessingLogConfig, SchemaAndValue> msgFactory) {
      this.config = config;
      this.msgFactory = msgFactory;
    }

    @Override
    public SchemaAndValue get() {
      final SchemaAndValue msg = msgFactory.apply(config);
      if (msg.schema().equals(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA)) {
        return msg;
      }
      throw new RuntimeException("Received message with invalid schema");
    }
  }

  public ProcessingLoggerImpl(final ProcessingLogConfig config, final StructuredLogger inner) {
    this.config = config;
    this.inner = inner;
  }

  public void error(final Function<ProcessingLogConfig, SchemaAndValue> msgFactory) {
    inner.error(new ProcessingLogMessage(config, msgFactory));
  }
}
