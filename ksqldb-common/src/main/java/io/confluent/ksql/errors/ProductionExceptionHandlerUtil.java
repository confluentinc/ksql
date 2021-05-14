/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

public final class ProductionExceptionHandlerUtil {
  public static final String KSQL_PRODUCTION_ERROR_LOGGER = "ksql.logger.production.error";

  private ProductionExceptionHandlerUtil() {
  }

  public static Class<?> getHandler(final boolean failOnError) {
    return failOnError
        ? LogAndFailProductionExceptionHandler.class
        : LogAndContinueProductionExceptionHandler.class;
  }

  abstract static class LogAndXProductionExceptionHandler implements ProductionExceptionHandler {

    private ProcessingLogger logger;

    @Override
    public ProductionExceptionHandlerResponse handle(
        final ProducerRecord<byte[], byte[]> record, final Exception exception) {
      logger.error(new ProductionError(exception.getMessage()));
      return getResponse();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
      final Object logger = configs.get(KSQL_PRODUCTION_ERROR_LOGGER);
      if (!(logger instanceof ProcessingLogger)) {
        throw new IllegalArgumentException("Invalid value for logger: " + logger.toString());
      }
      this.logger = (ProcessingLogger) logger;
    }

    abstract ProductionExceptionHandlerResponse getResponse();
  }

  public static final class ProductionError implements ProcessingLogger.ErrorMessage {

    private final String errorMsg;

    public ProductionError(final String errorMsg) {
      this.errorMsg = errorMsg == null ? "" : errorMsg;
    }

    @Override
    public SchemaAndValue get(final ProcessingLogConfig config) {
      final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA)
          .put(ProcessingLogMessageSchema.TYPE, MessageType.PRODUCTION_ERROR.getTypeId())
          .put(ProcessingLogMessageSchema.PRODUCTION_ERROR, productionError());

      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ProductionError that = (ProductionError) o;
      return Objects.equals(errorMsg, that.errorMsg);
    }

    @Override
    public int hashCode() {
      return Objects.hash(errorMsg);
    }

    private Struct productionError() {
      return new Struct(MessageType.PRODUCTION_ERROR.getSchema())
          .put(ProcessingLogMessageSchema.PRODUCTION_ERROR_FIELD_MESSAGE, errorMsg);
    }
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


