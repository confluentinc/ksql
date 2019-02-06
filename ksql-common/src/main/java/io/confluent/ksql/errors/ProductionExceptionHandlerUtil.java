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

package io.confluent.ksql.errors;

import io.confluent.common.logging.StructuredLogger;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.processing.log.ProcessingLoggerFactory;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

public final class ProductionExceptionHandlerUtil {
  public static final String KSQL_PRODUCTION_ERROR_LOGGER_NAME =
      "ksql.logger.production.error.name";

  private ProductionExceptionHandlerUtil() {
  }

  public static Class<?> getHandler(final boolean failOnError) {
    return failOnError
        ? LogAndFailProductionExceptionHandler.class
        : LogAndContinueProductionExceptionHandler.class;
  }

  abstract static class LogAndXProductionExceptionHandler implements ProductionExceptionHandler {

    private StructuredLogger logger;

    @Override
    public ProductionExceptionHandlerResponse handle(
        final ProducerRecord<byte[], byte[]> record, final Exception exception) {
      logger.error(productionError(exception.getMessage()));
      return getResponse();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
      configure(configs, ProcessingLoggerFactory::getLogger);
    }

    void configure(
        final Map<String, ?> configs, final Function<String, StructuredLogger> loggerFactory) {
      final String loggerName = configs.get(KSQL_PRODUCTION_ERROR_LOGGER_NAME).toString();
      logger = loggerFactory.apply(loggerName);
    }

    abstract ProductionExceptionHandlerResponse getResponse();
  }

  private static Supplier<SchemaAndValue> productionError(final String errorMsg) {
    return () -> {
      final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
      struct.put(ProcessingLogMessageSchema.TYPE, MessageType.PRODUCTION_ERROR.getTypeId());
      final Struct productionError =
          new Struct(MessageType.PRODUCTION_ERROR.getSchema());
      struct.put(ProcessingLogMessageSchema.PRODUCTION_ERROR, productionError);
      productionError.put(
          ProcessingLogMessageSchema.PRODUCTION_ERROR_FIELD_MESSAGE,
          errorMsg);
      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
    };
  }

  public static class LogAndFailProductionExceptionHandler
      extends LogAndXProductionExceptionHandler {

    @Override
    ProductionExceptionHandlerResponse getResponse() {
      return ProductionExceptionHandlerResponse.FAIL;
    }
  }

  public static class LogAndContinueProductionExceptionHandler
      extends LogAndXProductionExceptionHandler {

    @Override
    ProductionExceptionHandlerResponse getResponse() {
      return ProductionExceptionHandlerResponse.CONTINUE;
    }
  }
}


