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

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.metrics.Metrics;


public interface ProcessingLogContext {
  /**
   * @return The processing log config used by instances contained within this context.
   */
  ProcessingLogConfig getConfig();

  /**
   * @return A factory for creating structured loggers for logging processing log records.
   */
  ProcessingLoggerFactory getLoggerFactory();

  /**
   * Creates a processing log context that uses the supplied config.
   * @param config the processing log config
   * @param metrics the object that emits metrics
   * @param metricsTags the metricsTags to include with the metrics
   * @return A processing log context that uses the supplied config and emits metrics
   */
  static ProcessingLogContext create(
      final ProcessingLogConfig config,
      final Metrics metrics,
      final Map<String, String> metricsTags
  ) {
    return new ProcessingLogContextImpl(config, metrics, metricsTags);
  }

  /**
   * Creates a processing log context that uses the default processing log config.
   * @return A processing log context that uses the default config and doesn't emit metrics
   */
  static ProcessingLogContext create() {
    return new ProcessingLogContextImpl(
        new ProcessingLogConfig(Collections.emptyMap()),
        null,
        Collections.emptyMap()
    );
  }
}
