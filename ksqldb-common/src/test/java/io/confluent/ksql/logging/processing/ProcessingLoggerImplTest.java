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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.common.logging.StructuredLogger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingLoggerImplTest {
  @Mock
  private StructuredLogger innerLogger;
  @Mock
  private ProcessingLogConfig processingLogConfig;
  @Mock
  private Function<ProcessingLogConfig, SchemaAndValue> msgFactory;
  @Mock
  private SchemaAndValue msg;
  @SuppressWarnings("unchecked")
  private final ArgumentCaptor<Supplier<SchemaAndValue>> msgCaptor
      = ArgumentCaptor.forClass(Supplier.class);

  private ProcessingLogger processingLogger;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() {
    when(msgFactory.apply(any())).thenReturn(msg);
    when(msg.schema()).thenReturn(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
    processingLogger = new ProcessingLoggerImpl(processingLogConfig, innerLogger);
  }

  private final SchemaAndValue verifyErrorMessage() {
    verify(innerLogger).error(msgCaptor.capture());
    return msgCaptor.getValue().get();
  }

  @Test
  public void shouldLogError() {
    // When:
    processingLogger.error(msgFactory);

    // Then:
    final SchemaAndValue msg = verifyErrorMessage();
    assertThat(msg, is(this.msg));
  }

  @Test
  public void shouldBuildMessageUsingConfig() {
    // When:
    processingLogger.error(msgFactory);

    // Then:
    verifyErrorMessage();
    verify(msgFactory).apply(processingLogConfig);
  }

  @Test
  public void shouldThrowOnBadSchema() {
    // Given:
    when(msg.schema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    // Then:
    expectedException.expect(RuntimeException.class);

    // When:
    processingLogger.error(msgFactory);
    verifyErrorMessage();
  }
}