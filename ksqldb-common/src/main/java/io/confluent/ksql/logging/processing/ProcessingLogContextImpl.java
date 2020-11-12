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
import java.util.Enumeration;
import org.apache.kafka.log4jappender.KafkaLog4jAppender;
import org.apache.log4j.Logger;

public final class ProcessingLogContextImpl implements ProcessingLogContext {
  private final ProcessingLogConfig config;
  private final ProcessingLoggerFactory loggerFactory;

  ProcessingLogContextImpl(final ProcessingLogConfig config) {
    configureLogger();

    this.config = config;
    this.loggerFactory = new ProcessingLoggerFactoryImpl(
        config,
        new StructuredLoggerFactory(ProcessingLogConstants.PREFIX)
    );
  }

  private static void configureLogger() {
    final Logger processingLogger = Logger.getLogger(ProcessingLogConstants.PREFIX);

    final Enumeration appenders = processingLogger.getAllAppenders();
    while (appenders.hasMoreElements()) {
      final Object appender = appenders.nextElement();
      if (appender instanceof KafkaLog4jAppender) {
        final KafkaLog4jAppender kafkaLog4jAppender = (KafkaLog4jAppender) appender;
        kafkaLog4jAppender.setBrokerList("localhost:9092");
        kafkaLog4jAppender.setTopic("default_ksql_processing_log_programmatically");
      }
    }
  }

  public ProcessingLogConfig getConfig() {
    return config;
  }

  @Override
  public ProcessingLoggerFactory getLoggerFactory() {
    return loggerFactory;
  }
}
