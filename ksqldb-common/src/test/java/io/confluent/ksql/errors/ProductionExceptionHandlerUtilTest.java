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

package io.confluent.ksql.errors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil.LogAndContinueProductionExceptionHandler;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil.LogAndFailProductionExceptionHandler;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil.LogAndXProductionExceptionHandler;
import io.confluent.ksql.errors.ProductionExceptionHandlerUtil.ProductionError;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.Response;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.Result;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProductionExceptionHandlerUtilTest {

  @Mock
  private ProcessingLogger logger;
  @Mock
  private ProducerRecord<byte[], byte[]> record;
  @Mock
  private ErrorHandlerContext context;

  private final Response response = Response.resume();

  private Map<String, ?> configs;

  private final ProcessingLogConfig processingLogConfig = new ProcessingLogConfig(
      Collections.emptyMap());

  private LogAndXProductionExceptionHandler exceptionHandler;

  @Before
  public void setUp() {
    configs = ImmutableMap.of(
        ProductionExceptionHandlerUtil.KSQL_PRODUCTION_ERROR_LOGGER, logger);

    exceptionHandler = new TestLogAndXProductionExceptionHandler(response);
    exceptionHandler.configure(configs);
  }

  @Test
  public void shouldReturnLogAndFailHandler() {
    assertThat(
        ProductionExceptionHandlerUtil.getHandler(true),
        equalTo(LogAndFailProductionExceptionHandler.class));
  }

  @Test
  public void shouldReturnLogAndContinueHandler() {
    assertThat(
        ProductionExceptionHandlerUtil.getHandler(false),
        equalTo(LogAndContinueProductionExceptionHandler.class));
  }

  @Test
  public void shouldLogErrorCorrectly() {
    // When:
    exceptionHandler.handleError(context, record, new Exception("foo"));

    // Then:
    verify(logger).error(new ProductionError("foo"));
  }

  @Test
  public void shouldFormatProductionErrorCorrectly() {
    // Given:
    final ProductionError error = new ProductionError("foo");

    // When:
    final SchemaAndValue schemaAndValue = error.get(processingLogConfig);

    // Then:
    assertThat(schemaAndValue.schema(), is(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA));
    final Struct msg = (Struct) schemaAndValue.value();
    assertThat(
        msg.get(ProcessingLogMessageSchema.TYPE),
        is(MessageType.PRODUCTION_ERROR.getTypeId()));
    assertThat(msg.get(ProcessingLogMessageSchema.PRODUCTION_ERROR), notNullValue());
    final Struct productionError = msg.getStruct(ProcessingLogMessageSchema.PRODUCTION_ERROR);
    assertThat(productionError.schema(), is(MessageType.PRODUCTION_ERROR.getSchema()));
    assertThat(
        productionError.get(ProcessingLogMessageSchema.PRODUCTION_ERROR_FIELD_MESSAGE), is("foo"));
  }

  @Test
  public void shouldReturnCorrectResponse() {
    // When:
    final Response result = exceptionHandler.handleError(context, record, new Exception());

    // Then:
    assertThat(result, is(sameInstance(response)));
  }

  @Test
  public void shouldReturnFailFromLogAndFailHandler() {
    // Given:
    exceptionHandler = new LogAndFailProductionExceptionHandler();
    exceptionHandler.configure(configs);

    // When:
    final Response result = exceptionHandler.handleError(context, record, new Exception());

    // Then:
    assertThat(result.result(), is(Result.FAIL));
  }

  @Test
  public void shouldReturnContinueFromLogAndContinueHandler() {
    // Given:
    exceptionHandler = new LogAndContinueProductionExceptionHandler();
    exceptionHandler.configure(configs);

    // When:
    final Response result = exceptionHandler.handleError(context, record, new Exception());

    // Then:
    assertThat(result.result(), is(Result.RESUME));
  }

  private static class TestLogAndXProductionExceptionHandler extends LogAndXProductionExceptionHandler {

    private final Response response;

    private TestLogAndXProductionExceptionHandler(final Response response) {
      this.response = response;
    }

    @Override
    Response getResponse() {
      return response;
    }
  }
}