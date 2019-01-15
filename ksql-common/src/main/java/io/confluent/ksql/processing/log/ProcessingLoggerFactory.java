/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.common.logging.StructuredLogger;
import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Collection;

public final class ProcessingLoggerFactory {
  public static final String DELIMITER = ".";
  public static final String PREFIX = "processing";
  private static final StructuredLoggerFactory FACTORY = new StructuredLoggerFactory(PREFIX);

  private ProcessingLoggerFactory() {
  }

  public static StructuredLogger getLogger(final Class<?> clazz) {
    return FACTORY.getLogger(clazz);
  }

  public static StructuredLogger getLogger(final String name) {
    return FACTORY.getLogger(name);
  }

  public static Collection<String> getLoggers() {
    return FACTORY.getLoggers();
  }
}
