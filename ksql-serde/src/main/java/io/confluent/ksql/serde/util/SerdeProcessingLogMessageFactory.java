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

package io.confluent.ksql.serde.util;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public final class SerdeProcessingLogMessageFactory {
  private SerdeProcessingLogMessageFactory() {
  }

  public static Function<ProcessingLogConfig, SchemaAndValue> deserializationErrorMsg(
      final Throwable exception,
      final Optional<byte[]> record
  ) {
    Objects.requireNonNull(exception);
    return (config) -> {
      final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
      final Struct deserializationError = new Struct(MessageType.DESERIALIZATION_ERROR.getSchema());
      deserializationError.put(
          ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_MESSAGE,
          exception.getMessage());
      if (config.getBoolean(ProcessingLogConfig.INCLUDE_ROWS)) {
        deserializationError.put(
            ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_RECORD_B64,
            record.map(Base64.getEncoder()::encodeToString).orElse(null)
        );
      }
      struct.put(ProcessingLogMessageSchema.DESERIALIZATION_ERROR, deserializationError);
      struct.put(ProcessingLogMessageSchema.TYPE, MessageType.DESERIALIZATION_ERROR.getTypeId());
      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
    };
  }
}
