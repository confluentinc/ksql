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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class ProcessingLogMessageSchema {
  private ProcessingLogMessageSchema() {
  }

  private static final String NAMESPACE = "io.confluent.ksql.processing.log.";

  public enum MessageType {
    DESERIALIZATION_ERROR,
    RECORD_PROCESSING_ERROR
  }

  public static final String DESERIALIZATION_ERROR_FIELD_MESSAGE = "errorMessage";
  public static final String DESERIALIZATION_ERROR_FIELD_RECORD_B64 = "recordB64";

  public static final Schema DESERIALIZATION_ERROR_SCHEMA = SchemaBuilder.struct()
      .name(NAMESPACE + "DeserializationError")
      .field(DESERIALIZATION_ERROR_FIELD_MESSAGE, Schema.OPTIONAL_STRING_SCHEMA)
      .field(DESERIALIZATION_ERROR_FIELD_RECORD_B64, Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  public static final String RECORD_PROCESSING_ERROR_FIELD_MESSAGE = "errorMessage";
  public static final String RECORD_PROCESSING_ERROR_FIELD_RECORD = "record";

  public static final Schema RECORD_PROCESSING_ERROR_SCHEMA = SchemaBuilder.struct()
      .name(NAMESPACE + "RecordProcessingError")
      .field(RECORD_PROCESSING_ERROR_FIELD_MESSAGE, Schema.OPTIONAL_STRING_SCHEMA)
      .field(RECORD_PROCESSING_ERROR_FIELD_RECORD, Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  public static final String TYPE = "type";
  public static final String DESERIALIZATION_ERROR = "deserializationError";
  public static final String RECORD_PROCESSING_ERROR = "recordProcessingError";

  public static final Schema PROCESSING_LOG_SCHEMA = SchemaBuilder.struct()
      .name(NAMESPACE + "ProcessingLogRecord")
      .field(TYPE, Schema.OPTIONAL_INT32_SCHEMA)
      .field(DESERIALIZATION_ERROR, DESERIALIZATION_ERROR_SCHEMA)
      .field(RECORD_PROCESSING_ERROR, RECORD_PROCESSING_ERROR_SCHEMA)
      .optional()
      .build();

}
