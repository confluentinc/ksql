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

import org.apache.kafka.connect.data.SchemaAndValue;

public interface ProcessingLogger {

  /**
   * The interface all error message types must implement.
   */
  interface ErrorMessage {

    /**
     * Called to convert the error message into a structured message.
     *
     * <p>The returned value should use the {@link ProcessingLogMessageSchema}.
     *
     * <p>Implementations should lazily initialize to message being returned as construction
     * of the message happens on performance critical path.
     *
     * @param config the processing config.
     * @return the schema and structured error message.
     */
    SchemaAndValue get(ProcessingLogConfig config);
  }

  /**
   * Log a message at error level
   *
   * @param msg the error to log
   */
  void error(ErrorMessage msg);

  /**
   * Close the processing logger
   */
  void close();
}
