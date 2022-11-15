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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import java.util.Collections;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaStreamsThreadErrorTest {
  private final ProcessingLogConfig config = new ProcessingLogConfig(Collections.emptyMap());

  private Thread thread = new Thread("thread-1");
  private Throwable error = new Exception("cause0");

  @Test
  public void shouldBuildKafkaStreamThreadErrorCorrectly() {
    // Given:
    final ErrorMessage errorMessage = KafkaStreamsThreadError.of("errorMsg", thread, error);

    // When:
    final SchemaAndValue msgAndSchema = errorMessage.get(config);

    // Then:
    assertThat(msgAndSchema.schema(), equalTo(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct msg = (Struct) msgAndSchema.value();
    assertThat(
        msg.get(ProcessingLogMessageSchema.TYPE),
        equalTo(ProcessingLogMessageSchema.MessageType.KAFKA_STREAMS_THREAD_ERROR.getTypeId())
    );
    assertThat(
        msg.get(ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR),
        notNullValue()
    );
    final Struct kafkaStreamsThreadError =
        msg.getStruct(ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR);
    assertThat(
        kafkaStreamsThreadError.get(
            ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR_FIELD_MESSAGE),
        equalTo("errorMsg")
    );
    assertThat(
        kafkaStreamsThreadError.get(
            ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR_FIELD_NAME),
        equalTo("thread-1")
    );
    assertThat(
        kafkaStreamsThreadError.get(
            ProcessingLogMessageSchema.KAFKA_STREAMS_THREAD_ERROR_FIELD_CAUSE),
        equalTo(ErrorMessageUtil.getErrorMessages(error))
    );
  }
}
