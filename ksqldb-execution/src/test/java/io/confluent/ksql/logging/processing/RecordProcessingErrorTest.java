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

package io.confluent.ksql.logging.processing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class RecordProcessingErrorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String errorMsg = "error msg";
  private static final Throwable cause = new Exception("cause1", new Exception("cause2"));
  private static final Throwable error = new Exception("cause0", cause);

  private final ProcessingLogConfig config = new ProcessingLogConfig(
      Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS, true)
  );

  @Test
  public void shouldBuildRecordProcessingErrorCorrectly() throws IOException {
    // Given:
    final ErrorMessage errorMessage = RecordProcessingError.recordProcessingError(
        errorMsg, error, GenericRow.genericRow(123, "data")
    );

    // When:
    final SchemaAndValue msgAndSchema = errorMessage.get(config);

    // Then:
    assertThat(msgAndSchema.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct msg = (Struct) msgAndSchema.value();
    assertThat(
        msg.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.getTypeId()));
    assertThat(msg.get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR), notNullValue());
    final Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(errorMsg));
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_CAUSE),
        equalTo(ErrorMessageUtil.getErrorMessages(error)));
    final List<?> rowAsList =
        OBJECT_MAPPER.readValue(
            recordProcessingError.getString(
                ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD),
            List.class
        );
    assertThat(rowAsList, contains(123, "data"));
  }

  @Test
  public void shouldBuildRecordProcessingErrorCorrectlyIfRowNull() {
    // Given:
    final ErrorMessage errorMessage = RecordProcessingError.recordProcessingError(
        errorMsg, error, (GenericRow) null
    );

    // When:
    final SchemaAndValue msgAndSchema = errorMessage.get(config);

    // Then:
    final Struct msg = (Struct) msgAndSchema.value();
    final Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD),
        nullValue());
  }

  @Test
  public void shouldBuildRecordProcessingErrorCorrectlyIfNoException() {
    // Given:
    final ErrorMessage errorMessage = RecordProcessingError.recordProcessingError(
        errorMsg, null
    );

    // When:
    final SchemaAndValue msgAndSchema = errorMessage.get(config);

    // Then:
    final Struct msg = (Struct) msgAndSchema.value();
    final Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_CAUSE),
        equalTo(Collections.emptyList()));
  }

  @Test
  public void shouldBuildRecordProcessingErrorWithNullRowIfIncludeRowsFalse() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS,  false)
    );

    final ErrorMessage errorMessage = RecordProcessingError.recordProcessingError(
        errorMsg, error, GenericRow.genericRow(123, "data")
    );

    // When:
    final SchemaAndValue msgAndSchema = errorMessage.get(config);

    // Then:
    final Struct msg = (Struct) msgAndSchema.value();
    final Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD),
        nullValue());
  }
}