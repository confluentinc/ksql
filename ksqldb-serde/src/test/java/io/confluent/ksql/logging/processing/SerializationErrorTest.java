/*
 * Copyright 2020 Confluent Inc.
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

import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.SERIALIZATION_ERROR;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_CAUSE;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_MESSAGE;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_RECORD;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.SERIALIZATION_ERROR_FIELD_TOPIC;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class SerializationErrorTest {

  private static final String TOPIC = "topic";
  private static final GenericRow RECORD = genericRow("some", "fields");
  private static final Exception CAUSE = new Exception("cause1", new Exception("cause2"));
  private static final Exception ERROR = new Exception("error message", CAUSE);
  private static final List<String> CAUSE_LIST = ImmutableList.of("cause1", "cause2");

  private static final ProcessingLogConfig LOGGING_CONFIG = new ProcessingLogConfig(
      Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS, true)
  );

  @Test
  public void shouldBuildSerializationError() {
    // Given:
    final SerializationError<GenericRow> serError = new SerializationError<>(
        ERROR,
        Optional.of(RECORD),
        TOPIC
    );

    // When:
    final SchemaAndValue msg = serError.get(LOGGING_CONFIG);

    // Then:
    final Schema schema = msg.schema();
    assertThat(schema, equalTo(PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) msg.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(ProcessingLogMessageSchema.MessageType.SERIALIZATION_ERROR.getTypeId()));
    final Struct serializationError = struct.getStruct(SERIALIZATION_ERROR);
    assertThat(
        serializationError.get(SERIALIZATION_ERROR_FIELD_MESSAGE),
        equalTo(ERROR.getMessage())
    );
    assertThat(
        serializationError.get(SERIALIZATION_ERROR_FIELD_CAUSE),
        equalTo(CAUSE_LIST)
    );
    assertThat(
        serializationError.get(SERIALIZATION_ERROR_FIELD_RECORD),
        equalTo(RECORD.toString())
    );
    assertThat(
        serializationError.get(SERIALIZATION_ERROR_FIELD_TOPIC),
        equalTo(TOPIC)
    );
    schema.fields().forEach(
        f -> {
          if (!ImmutableList.of(TYPE, SERIALIZATION_ERROR).contains(f.name())) {
            assertThat(struct.get(f), is(nullValue()));
          }
        }
    );
  }

  @Test
  public void shouldSetEmptyRecordToNull() {
    // Given:
    final SerializationError<GenericRow> serError =
        new SerializationError<>(ERROR, Optional.empty(), TOPIC);

    // When:
    final SchemaAndValue msg = serError.get(LOGGING_CONFIG);

    // Then:
    final Struct struct = (Struct) msg.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(ProcessingLogMessageSchema.MessageType.SERIALIZATION_ERROR.getTypeId()));
    final Struct serializationError = struct.getStruct(SERIALIZATION_ERROR);
    assertThat(serializationError.get(SERIALIZATION_ERROR_FIELD_RECORD), is(nullValue()));
  }

  @Test
  public void shouldBuildSerializationErrorWithNullRecordIfIncludeRowFalse() {
    // Given:
    final ProcessingLogConfig config = new ProcessingLogConfig(
        Collections.singletonMap(ProcessingLogConfig.INCLUDE_ROWS, false)
    );

    final SerializationError<GenericRow> serError = new SerializationError<>(
        ERROR,
        Optional.of(RECORD),
        TOPIC
    );

    // When:
    final SchemaAndValue msg = serError.get(config);

    // Then:
    final Struct struct = (Struct) msg.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(ProcessingLogMessageSchema.MessageType.SERIALIZATION_ERROR.getTypeId()));
    final Struct serializationError = struct.getStruct(SERIALIZATION_ERROR);
    assertThat(serializationError.get(SERIALIZATION_ERROR_FIELD_RECORD), is(nullValue()));
  }
}