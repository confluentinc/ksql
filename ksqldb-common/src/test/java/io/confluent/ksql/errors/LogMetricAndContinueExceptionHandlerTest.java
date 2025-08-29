/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.ksql.errors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LogMetricAndContinueExceptionHandlerTest {

  private LogMetricAndContinueExceptionHandler exceptionHandler;

  @Mock
  private StreamsErrorCollector streamsErrorCollector;
  @Mock
  private ProcessorContext context;
  @Mock
  private ConsumerRecord<byte[], byte[]> record;

  @Before
  public void setUp() {
    final Map<String, ?> configs = ImmutableMap.of(
        KsqlConfig.KSQL_INTERNAL_STREAMS_ERROR_COLLECTOR_CONFIG, streamsErrorCollector);

    exceptionHandler = new LogMetricAndContinueExceptionHandler();
    exceptionHandler.configure(configs);
  }

  @Test
  public void shouldCallErrorCollector() {
    when(record.topic()).thenReturn("test");
    exceptionHandler.handle(context, record, mock(Exception.class));
    verify(streamsErrorCollector).recordError("test");
  }

  @Test
  public void shouldReturnContinueForRegularExceptions() {
    assertThat(exceptionHandler.handle(context, record, mock(Exception.class)),
        equalTo(DeserializationHandlerResponse.CONTINUE));
  }

  @Test
  public void shouldReturnFailForAuthorizationExceptions() {
    assertThat(exceptionHandler.handle(context, record,
            new Exception("", new RestClientException("", 403, 40301))),
        equalTo(DeserializationHandlerResponse.FAIL));
  }

  @Test
  public void shouldReturnContinueForSelfReferencingExceptions() {
    assertThat(exceptionHandler.handle(context, record, new Exception() {
      @Override
      public synchronized Throwable getCause() {
        return this;
      }
    }), equalTo(DeserializationHandlerResponse.CONTINUE));
  }

}
