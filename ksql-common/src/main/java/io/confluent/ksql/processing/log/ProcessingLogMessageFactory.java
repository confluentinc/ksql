/*
 * Copyright 2018 Confluent Inc.
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

import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public final class ProcessingLogMessageFactory {
  private ProcessingLogMessageFactory() {
  }

  private static final String NAMESPACE = "io.confluent.ksql.processing.log.";

  public enum MessageType {
    DESERIALIZATION_ERROR,
  }

  static String DESERIALIZATION_ERROR_FIELD_MESSAGE = "message";
  static String DESERIALIZATION_ERROR_FIELD_RECORD = "record";

  private static final Schema DESERIALIZATION_ERROR_SCHEMA = SchemaBuilder.struct()
      .name(NAMESPACE + "DeserializationError")
      .field(DESERIALIZATION_ERROR_FIELD_MESSAGE, Schema.OPTIONAL_STRING_SCHEMA)
      .field(DESERIALIZATION_ERROR_FIELD_RECORD, Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  static String TYPE = "type";
  static String DESERIALIZATION_ERROR = "deserializationError";

  public static final Schema PROCESSING_LOG_SCHEMA = SchemaBuilder.struct()
      .name(NAMESPACE + "ProcessingLogRecord")
      .field(TYPE, Schema.OPTIONAL_INT32_SCHEMA)
      .field(DESERIALIZATION_ERROR, DESERIALIZATION_ERROR_SCHEMA)
      .optional()
      .build();

  public static Supplier<SchemaAndValue> deserializationErrorMsg(
      final Throwable exception,
      final Optional<byte[]> record) {
    Objects.requireNonNull(exception);
    return () -> {
      final Struct struct = new Struct(PROCESSING_LOG_SCHEMA);
      final Struct deserializationError = new Struct(DESERIALIZATION_ERROR_SCHEMA);
      deserializationError.put(DESERIALIZATION_ERROR_FIELD_MESSAGE, exception.getMessage());
      deserializationError.put(
          DESERIALIZATION_ERROR_FIELD_RECORD,
          record.map(Base64.getEncoder()::encodeToString).orElse(null)
      );
      struct.put(DESERIALIZATION_ERROR, deserializationError);
      struct.put(TYPE, MessageType.DESERIALIZATION_ERROR.ordinal());
      return new SchemaAndValue(PROCESSING_LOG_SCHEMA, struct);
    };
  }
}
