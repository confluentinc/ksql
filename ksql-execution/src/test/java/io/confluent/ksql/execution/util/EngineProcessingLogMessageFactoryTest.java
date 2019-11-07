package io.confluent.ksql.execution.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class EngineProcessingLogMessageFactoryTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String errorMsg = "error msg";
  private static final Throwable cause = new Exception("cause1", new Exception("cause2"));
  private static final Throwable error = new Exception(errorMsg, cause);

  private final ProcessingLogConfig config = new ProcessingLogConfig(
      Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS,  true)
  );

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildRecordProcessingErrorCorrectly() throws IOException {
    // When:
    SchemaAndValue msgAndSchema = EngineProcessingLogMessageFactory.recordProcessingError(
        errorMsg, error, new GenericRow(123, "data")
    ).apply(config);

    // Then:
    assertThat(msgAndSchema.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    Struct msg = (Struct) msgAndSchema.value();
    assertThat(
        msg.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.RECORD_PROCESSING_ERROR.getTypeId()));
    assertThat(msg.get(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR), notNullValue());
    Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_MESSAGE),
        equalTo(errorMsg));
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_CAUSE),
        equalTo(ErrorMessageUtil.getErrorMessages(cause)));
    List<Object> rowAsList =
        OBJECT_MAPPER.readValue(
            recordProcessingError.getString(
                ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD),
            List.class
        );
    assertThat(rowAsList, contains(123, "data"));
  }

  @Test
  public void shouldBuildRecordProcessingErrorCorrectlyIfRowNull() {
    // When:
    SchemaAndValue msgAndSchema = EngineProcessingLogMessageFactory.recordProcessingError(
        errorMsg, error, null
    ).apply(config);

    // Then:
    Struct msg = (Struct) msgAndSchema.value();
    Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD),
        nullValue());
  }

  @Test
  public void shouldBuildRecordProcessingErrorWithNullRowIfIncludeRowsFalse() {
    // Given:
    ProcessingLogConfig config = new ProcessingLogConfig(
        Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS,  false)
    );

    // When:
    SchemaAndValue msgAndSchema = EngineProcessingLogMessageFactory.recordProcessingError(
        errorMsg, error, new GenericRow(123, "data")
    ).apply(config);

    // Then:
    Struct msg = (Struct) msgAndSchema.value();
    Struct recordProcessingError =
        msg.getStruct(ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR);
    assertThat(
        recordProcessingError.get(
            ProcessingLogMessageSchema.RECORD_PROCESSING_ERROR_FIELD_RECORD),
        nullValue());
  }
}