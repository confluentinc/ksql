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

package io.confluent.ksql.errors;

import io.confluent.ksql.metrics.StreamsErrorCollector;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogMetricAndContinueExceptionHandler implements DeserializationExceptionHandler {
  private static final Logger log
      = LoggerFactory.getLogger(LogMetricAndContinueExceptionHandler.class);

  @Override
  public DeserializationHandlerResponse handle(
      final ProcessorContext context,
      final ConsumerRecord<byte[], byte[]> record,
      final Exception exception
  ) {
    log.debug(
        "Exception caught during Deserialization, "
        + "taskId: {}, topic: {}, partition: {}, offset: {}",
        context.taskId(), record.topic(), record.partition(), record.offset(),
        exception
    );

    StreamsErrorCollector.recordError(context.applicationId(), record.topic());
    return DeserializationHandlerResponse.CONTINUE;
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    // ignore
  }
}
