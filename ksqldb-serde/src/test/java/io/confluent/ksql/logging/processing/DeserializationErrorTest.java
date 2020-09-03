/*
 * Copyright 2019 Confluent Inc.
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

import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_CAUSE;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_TARGET;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_MESSAGE;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_RECORD_B64;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_TOPIC;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA;
import static io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
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
        "topic",
        false
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Struct struct = (Struct) msg.value();
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    assertThat(deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64),
        is(nullValue()));
  }

  @Test
  public void shouldBuildDeserializationError() {
    // Given:
    final DeserializationError deserError = new DeserializationError(
        error,
        Optional.of(record),
        "topic",
        false
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Schema schema = msg.schema();
    assertThat(schema, equalTo(PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) msg.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.DESERIALIZATION_ERROR.getTypeId()));
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_TARGET),
        equalTo("value")
    );
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_MESSAGE),
        equalTo(error.getMessage())
    );
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_CAUSE),
        equalTo(causeList)
    );
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64),
        equalTo(Base64.getEncoder().encodeToString(record))
    );
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_TOPIC),
        equalTo("topic")
    );
    schema.fields().forEach(
        f -> {
          if (!ImmutableList.of(TYPE, DESERIALIZATION_ERROR).contains(f.name())) {
            assertThat(struct.get(f), is(nullValue()));
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
        "topic",
        false
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Struct struct = (Struct) msg.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.DESERIALIZATION_ERROR.getTypeId()));
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    assertThat(deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64), nullValue());
  }

  @Test
  public void shouldBuildErrorWithKeyComponent() {
    // Given:
    final DeserializationError deserError = new DeserializationError(
        error,
        Optional.of(record),
        "topic",
        true
    );

    // When:
    final SchemaAndValue msg = deserError.get(config);

    // Then:
    final Struct struct = (Struct) msg.value();
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_TARGET),
        equalTo("key")
    );
  }

  @Test
  public void shouldImplementHashCodeAndEquals() {
    final Exception error1 = new Exception("error");
    final Exception error2 = new Exception("different error");
    final byte[] record1 = new byte[256];
    final byte[] record2 = new byte[512];
    new EqualsTester()
        .addEqualityGroup(
            new DeserializationError(error1, Optional.of(record1), "topic", false),
            new DeserializationError(error1, Optional.of(record1), "topic", false)
        )
        .addEqualityGroup(
            new DeserializationError(error2, Optional.of(record1), "topic", false)
        )
        .addEqualityGroup(
            new DeserializationError(error1, Optional.of(record2), "topic", false)
        )
        .addEqualityGroup(
            new DeserializationError(error1, Optional.empty(), "topic", false)
        )
        .addEqualityGroup(
            new DeserializationError(error1, Optional.of(record1), "other_topic", false)
        )
        .addEqualityGroup(
            new DeserializationError(error1, Optional.of(record1), "topic", true)
        )
        .testEquals();
  }
}