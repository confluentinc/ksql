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

package io.confluent.ksql.processing.log;

import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Collections;

public interface ProcessingLogContext {
  /**
   * @return The processing log config used by instances contained within this context.
   */
  ProcessingLogConfig getConfig();

  /**
   * @return A factory for creating structured loggers for logging processing log records.
   */
  StructuredLoggerFactory getLoggerFactory();

  /**
   * Creates a processing log context that uses the supplied config.
   * @param config the processing log config
   * @return: A processing log context that uses the supplied config.
   */
  static ProcessingLogContext create(final ProcessingLogConfig config) {
    return new ProcessingLogContextImpl(config);
  }

  /**
   * Creates a processing log context that uses the default processing log config.
   * @return A processing log context that uses the default config
   */
  static ProcessingLogContext create() {
    return new ProcessingLogContextImpl(new ProcessingLogConfig(Collections.emptyMap()));
  }
}
