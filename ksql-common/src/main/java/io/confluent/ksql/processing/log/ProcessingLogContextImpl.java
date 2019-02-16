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

public final class ProcessingLogContextImpl implements ProcessingLogContext {
  private final ProcessingLogConfig config;
  private final StructuredLoggerFactory loggerFactory;

  ProcessingLogContextImpl(final ProcessingLogConfig config) {
    this.config = config;
    this.loggerFactory = new StructuredLoggerFactory(ProcessingLogConstants.PREFIX);
  }

  public ProcessingLogConfig getConfig() {
    return config;
  }

  public StructuredLoggerFactory getLoggerFactory() {
    return loggerFactory;
  }
}
