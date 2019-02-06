/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.errors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.logging.StructuredLogger;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LogAndFailProductionExceptionHandlerTest {

  private final static String LOGGER_NAME = "loggerName";
  private final static Map<String, ?> CONFIGS = ImmutableMap.of(
      LogAndXProductionExceptionHandler.KSQL_PRODUCTION_ERROR_LOGGER_NAME, LOGGER_NAME);

  @Mock
  private Function<String, StructuredLogger> loggerFactory;
  @Mock
  private StructuredLogger logger;
  @Mock
  private ProducerRecord<byte[], byte[]> record;

  private LogAndFailProductionExceptionHandler exceptionHandler;

  @Before
  public void setUp() {
    when(loggerFactory.apply(ArgumentMatchers.anyString())).thenReturn(logger);

    exceptionHandler = new LogAndFailProductionExceptionHandler();
    exceptionHandler.configure(CONFIGS, loggerFactory);
  }

  @Test
  public void shouldFailOnError() {
    // When:
    final ProductionExceptionHandlerResponse response =
        exceptionHandler.handle(record, new Exception());

    // Then:
    assertThat(response, is(ProductionExceptionHandlerResponse.FAIL));
  }
}