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

import com.google.common.base.Throwables;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.log4j.Logger;

public class LogMetricAndContinueExceptionHandler implements DeserializationExceptionHandler {

  private static final Logger log
      = Logger.getLogger(LogMetricAndContinueExceptionHandler.class);
  private StreamsErrorCollector streamsErrorCollector;

  @Override
  public DeserializationHandlerResponse handle(
      final ProcessorContext context,
      final ConsumerRecord<byte[], byte[]> record,
      final Exception exception
  ) {
    log.debug(
        String.format("Exception caught during Deserialization, "
            + "taskId: %s, topic: %s, partition: %d, offset: %d",
        context.taskId(), record.topic(), record.partition(), record.offset()),
        exception
    );

    streamsErrorCollector.recordError(record.topic());

    if (isCausedByAuthorizationError(exception)) {
      log.info(
          String.format(
              "Authorization error when attempting to access the schema during deserialization. "
              + "taskId: %s, topic: %s, partition: %d, offset: %d",
          context.taskId(), record.topic(), record.partition(), record.offset()));
      return DeserializationHandlerResponse.FAIL;
    }

    return DeserializationHandlerResponse.CONTINUE;
  }

  private boolean isCausedByAuthorizationError(final Throwable throwable) {
    try {
      final Throwable error = Throwables.getRootCause(throwable);
      return (error instanceof RestClientException
          && ((((RestClientException) error).getStatus() == 401)
          || ((RestClientException) error).getStatus() == 403));
    } catch (Throwable t) {
      return false;
    }
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    this.streamsErrorCollector =
        (StreamsErrorCollector) Objects.requireNonNull(
            configs.get(
                KsqlConfig.KSQL_INTERNAL_STREAMS_ERROR_COLLECTOR_CONFIG
            )
        );
  }
}
