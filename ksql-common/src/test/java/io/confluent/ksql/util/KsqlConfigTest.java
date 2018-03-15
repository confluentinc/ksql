/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.core.IsEqual;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KsqlConfigTest {

  @Test
  public void shouldSetInitialValuesCorrectly() {
    Map<String, Object> initialProps = new HashMap<>();
    initialProps.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 10);
    initialProps.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);
    initialProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 800);
    initialProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

    KsqlConfig ksqlConfig = new KsqlConfig(initialProps);

    assertThat(ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY), equalTo(10));
    assertThat(ksqlConfig.get(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY), equalTo((short) 3));

  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerByDefault() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, IsEqual.equalTo(LogMetricAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerWhenFailOnDeserializationErrorFalse() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, false));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, IsEqual.equalTo(LogMetricAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldNotSetDeserializationExceptionHandlerWhenFailOnDeserializationErrorTrue() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, true));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, nullValue());
  }

  @Test
  public void shouldSetStreamsConfigProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "100"));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("100"));
  }

  @Test
  public void shouldSetPrefixedStreamsConfigProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "100"));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("100"));
  }

  @Test
  public void shouldCloneWithKsqlPropertyOverwrite() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test"));
    final KsqlConfig ksqlConfigClone = ksqlConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test-2"));
    Object result = ksqlConfigClone.getKsqlConfigProps().get(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    assertThat(result, equalTo("test-2"));
  }

  @Test
  public void shouldCloneWithStreamPropertyOverwrite() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "100"));
    final KsqlConfig ksqlConfigClone = ksqlConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "200"));
    Object result = ksqlConfigClone.getKsqlStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("200"));
  }

  @Test
  public void shouldCloneWithPrefixedStreamPropertyOverwrite() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "100"));
    final KsqlConfig ksqlConfigClone = ksqlConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "200"));
    Object result = ksqlConfigClone.getKsqlStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("200"));
  }
}
