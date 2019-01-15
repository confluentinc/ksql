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

package io.confluent.ksql.serde;

import static io.confluent.ksql.processing.log.ProcessingLogMessageSchema.DESERIALIZATION_ERROR;
import static io.confluent.ksql.processing.log.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_MESSAGE;
import static io.confluent.ksql.processing.log.ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_RECORD_B64;
import static io.confluent.ksql.processing.log.ProcessingLogMessageSchema.TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static io.confluent.ksql.processing.log.ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema;
import io.confluent.ksql.processing.log.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.serde.util.SerdeProcessingLogMessageFactory;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

public class SerdeProcessingLogMessageFactoryTest {
  private final byte[] record = new byte[256];
  private final Exception error = new Exception("error message");

  @Before
  public void setup() {
    IntStream.range(0, 256).forEach(i -> record[i] = (byte) (Byte.MIN_VALUE + i));
  }

  @Test
  public void shouldSetNullRecordToNull() {
    // When:
    final SchemaAndValue msg = SerdeProcessingLogMessageFactory.deserializationErrorMsg(
        error,
        Optional.empty()
    ).get();

    // Then:
    final Struct struct = (Struct) msg.value();
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    assertThat(deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64), is(nullValue()));
  }

  @Test
  public void shouldBuildDeserializationError() {
    // When:
    final SchemaAndValue msg = SerdeProcessingLogMessageFactory.deserializationErrorMsg(
        error,
        Optional.of(record)
    ).get();

    // Then:
    final Schema schema = msg.schema();
    assertThat(schema, equalTo(PROCESSING_LOG_SCHEMA));
    final Struct struct = (Struct) msg.value();
    assertThat(
        struct.get(ProcessingLogMessageSchema.TYPE),
        equalTo(MessageType.DESERIALIZATION_ERROR.ordinal()));
    final Struct deserializationError = struct.getStruct(DESERIALIZATION_ERROR);
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_MESSAGE),
        equalTo(error.getMessage())
    );
    assertThat(
        deserializationError.get(DESERIALIZATION_ERROR_FIELD_RECORD_B64),
        equalTo(Base64.getEncoder().encodeToString(record))
    );
    schema.fields().forEach(
        f -> {
          if (!ImmutableList.of(TYPE, DESERIALIZATION_ERROR).contains(f.name())) {
            assertThat(struct.get(f), is(nullValue()));
          }
        }
    );
  }
}
