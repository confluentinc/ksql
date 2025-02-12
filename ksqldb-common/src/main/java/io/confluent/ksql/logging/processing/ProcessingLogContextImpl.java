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

import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Map;
import org.apache.kafka.common.metrics.Metrics;

public final class ProcessingLogContextImpl implements ProcessingLogContext {
  private final ProcessingLogConfig config;
  private final ProcessingLoggerFactory loggerFactory;

  ProcessingLogContextImpl(
      final ProcessingLogConfig config,
      final Metrics metrics,
      final Map<String, String> metricsTags) {
    this.config = config;
    this.loggerFactory = new MeteredProcessingLoggerFactory(
        config,
        new StructuredLoggerFactory(ProcessingLogConstants.PREFIX),
        metrics,
        metricsTags
    );
  }

  public ProcessingLogConfig getConfig() {
    return config;
  }

  @Override
  public ProcessingLoggerFactory getLoggerFactory() {
    return loggerFactory;
  }
}
