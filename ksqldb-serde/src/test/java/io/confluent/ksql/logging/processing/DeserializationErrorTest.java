package io.confluent.ksql.logging.processing;

import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_CAUSE;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_MESSAGE;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_RECORD_B64;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_TOPIC;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

public class DeserializationErrorTest {

  private final byte[] record = new byte[256];
  private final Exception cause = new Exception("cause1", new Exception("cause2"));
  private final Exception error = new Exception("error message", cause);
  private final List<String> causeList = ImmutableList.of("cause1", "cause2");

  private final ProcessingLogConfig config = new ProcessingLogConfig(
      Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS, true)
  );

  @Test
  public void shouldSetNullRecordToNull() {
    // Given:
    final DeserializationError deserError = new DeserializationError(
        error,
        Optional.empty(),
        "topic"
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Struct struct = (Struct) msg.value();
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    Assert.assertThat(deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64),
        is(nullValue()));
  }

  @Test
  public void shouldBuildDeserializationError() {
    // Given:
    final DeserializationError deserError = new DeserializationError(
        error,
        Optional.of(record),
        "topic"
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Schema schema = msg.schema();
    Assert.assertThat(schema, equalTo(PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) msg.value();
    Assert.assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.DESERIALIZATION_ERROR.getTypeId()));
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    Assert.assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_MESSAGE),
        equalTo(error.getMessage())
    );
    Assert.assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_CAUSE),
        equalTo(causeList)
    );
    Assert.assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64),
        equalTo(Base64.getEncoder().encodeToString(record))
    );
    Assert.assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_TOPIC),
        equalTo("topic")
    );
    schema.fields().forEach(
        f -> {
          if (!ImmutableList.of(TYPE, DESERIALIZATION_ERROR).contains(f.name())) {
            Assert.assertThat(struct.get(f), is(nullValue()));
          }
        }
    );
  }

  @Test
  public void shouldBuildDeserializationErrorWithNullRecordIfIncludeRowFalse() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS, false)
    );

    final DeserializationError deserError = new DeserializationError(
        error,
        Optional.of(record),
        "topic"
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Struct struct = (Struct) msg.value();
    Assert.assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.DESERIALIZATION_ERROR.getTypeId()));
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    Assert
        .assertThat(deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64), nullValue());
  }
}