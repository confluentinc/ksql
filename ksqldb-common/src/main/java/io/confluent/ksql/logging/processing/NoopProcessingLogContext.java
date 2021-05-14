/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;

/**
 * An implementation of {@code ProcessingLogContext} that does nothing.
 */
public final class NoopProcessingLogContext implements ProcessingLogContext {

  private static final ProcessingLogConfig NOOP_CONFIG = new ProcessingLogConfig(ImmutableMap.of());

  public static final ProcessingLogger NOOP_LOGGER = msgFactory -> { };

  private static final ProcessingLoggerFactory NOOP_FACTORY = new ProcessingLoggerFactory() {
    @Override
    public ProcessingLogger getLogger(final String name) {
      return NOOP_LOGGER;
    }

    @Override
    public Collection<String> getLoggers() {
      return ImmutableList.of();
    }
  };

  public static final ProcessingLogContext INSTANCE = new NoopProcessingLogContext();

  private NoopProcessingLogContext() {
  }

  @Override
  public ProcessingLogConfig getConfig() {
    return NOOP_CONFIG;
  }

  @Override
  public ProcessingLoggerFactory getLoggerFactory() {
    return NOOP_FACTORY;
  }
}
