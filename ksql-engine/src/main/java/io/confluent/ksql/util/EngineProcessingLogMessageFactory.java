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

package io.confluent.ksql.util;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EngineProcessingLogMessageFactory {
  private static final Logger LOGGER
      = LoggerFactory.getLogger(EngineProcessingLogMessageFactory.class);

  private EngineProcessingLogMessageFactory() {
  }

  public static Function<ProcessingLogConfig, SchemaAndValue> recordProcessingError(
      final String errorMsg,
      final GenericRow record
  ) {
    return (config) -> {
      final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
      struct.put(ProcessingLogMessageSchema.TYPE, MessageType.RECORD_PROCESSING_ERROR.getTypeId());
      final Struct recordProcessingError =
          new Struct(MessageType.RECORD_PROCESSING_ERROR.getSchema());
      struct.put(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR, recordProcessingError);
      recordProcessingError.put(
          ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE,
          errorMsg);
      if (record == null) {
        return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
      }
      recordProcessingError.put(
          ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD,
          serializeRow(config, record));
      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
    };
  }

  private static String serializeRow(final ProcessingLogConfig config, final GenericRow record) {
    if (!config.getBoolean(ProcessingLogConfig.INCLUDE_ROWS)) {
      return null;
    }
    try {
      return JsonMapper.INSTANCE.mapper.writeValueAsString(record.getColumns());
    } catch (final Throwable t) {
      LOGGER.error("error serializing record for processing log", t);
      return null;
    }
  }
}
