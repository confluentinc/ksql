/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.util;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class KsqlConfigTest {

  @Test
  public void shouldSetLogAndContinueExceptionHandlerByDefault() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerWhenFailOnDeserializationErrorFalse() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, false));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, equalTo(LogAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldNotSetDeserializationExceptionHandlerWhenFailOnDeserializationErrorTrue() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, true));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, nullValue());
  }

}