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

package io.confluent.ksql.logging.processing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.Logger;
import io.confluent.ksql.logging.processing.ProcessingLogger.ErrorMessage;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingLoggerImplTest {

  @Mock
  private Logger innerLogger;
  @Mock
  private ProcessingLogConfig processingLogConfig;
  @Mock
  private SchemaAndValue msg;
  @Mock
  private ErrorMessage errorMsg;
  @SuppressWarnings("unchecked")
  private final ArgumentCaptor<SerializableSchemaAndValue> msgCaptor
      = ArgumentCaptor.forClass(SerializableSchemaAndValue.class);

  private ProcessingLogger processingLogger;

  @Before
  public void setup() {
    when(errorMsg.get(any())).thenReturn(msg);
    when(msg.schema()).thenReturn(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
    processingLogger = new ProcessingLoggerImpl(processingLogConfig, innerLogger);
  }

  @Test
  public void shouldLogError() {
    // When:
    processingLogger.error(errorMsg);

    // Then:
    final SchemaAndValue msg = verifyErrorMessage();
    assertThat(msg, is(msg));
  }

  @Test
  public void shouldBuildMessageUsingConfig() {
    // When:
    processingLogger.error(errorMsg);

    // Then:
    verifyErrorMessage();
    verify(errorMsg).get(processingLogConfig);
  }

  @Test
  public void shouldThrowOnBadSchema() {
    // Given:
    when(msg.schema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    assertThrows(
        RuntimeException.class,
        () -> processingLogger.error(errorMsg)
    );

  }

  private SchemaAndValue verifyErrorMessage() {
    verify(innerLogger).error(msgCaptor.capture());
    return msgCaptor.getValue().getMessage();
  }
}