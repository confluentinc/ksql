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

import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;

public interface ProcessingLogger {
  /**
   * Log a message at error level
   * @param msgFactory A factory function that returns a structured log message given
   *                   an instance of ProcessingLogConfig
   */
  void error(Function<ProcessingLogConfig, SchemaAndValue> msgFactory);
}
