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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
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

    assertThat(ksqlConfig.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY), equalTo(10));
    assertThat(ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY), equalTo((short) 3));

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
  public void shouldSetStreamsConfigConsumerUnprefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "100"));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("100"));
  }

  @Test
  public void shouldSetStreamsConfigConsumerPrefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "100"));
    Object result = ksqlConfig.getKsqlStreamConfigProps().get(
        StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("100"));
  }

  @Test
  public void shouldSetStreamsConfigProducerUnprefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024"));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(ProducerConfig.BUFFER_MEMORY_CONFIG);
    assertThat(result, equalTo(1024L));
  }

  @Test
  public void shouldSetStreamsConfigProducerPrefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "1024"));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(
        StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG);
    assertThat(result, equalTo(1024L));
  }

  @Test
  public void shouldSetStreamsConfigAdminClientProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, 3));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(
        AdminClientConfig.RETRIES_CONFIG);
    assertThat(result, equalTo(3));
  }

  @Test
  public void shouldSetStreamsConfigProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "128"));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG);
    assertThat(result, equalTo(128L));
  }

  @Test
  public void shouldSetPrefixedStreamsConfigProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "128"));
    final Object result
        = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG);
    assertThat(result, equalTo(128L));
  }

  @Test
  public void shouldSetMonitoringInterceptorConfigProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        "confluent.monitoring.interceptor.topic", "foo"));
    final Object result
        = ksqlConfig.getKsqlStreamConfigProps().get("confluent.monitoring.interceptor.topic");
    assertThat(result, equalTo("foo"));
  }

  @Test
  public void shouldObfuscateSecretStreamsProperties() {
    final String password = "super-secret-password";
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        SslConfigs.SSL_KEY_PASSWORD_CONFIG, password
    ));
    final Password passwordConfig
        = (Password) ksqlConfig.getKsqlStreamConfigProps().get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    assertThat(passwordConfig.value(), equalTo(password));
    assertThat(
        ksqlConfig.getKsqlConfigPropsWithSecretsObfuscated().get(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
        not(equalTo(password))
    );
  }

  @Test
  public void shouldFilterPropertiesForWhichTypeUnknown() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap("you.shall.not.pass", "wizard"));
    assertThat(
        ksqlConfig.getAllConfigPropsWithSecretsObfuscated().keySet(),
        not(hasItem("you.shall.not.pass")));
  }

  @Test
  public void shouldCloneWithKsqlPropertyOverwrite() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test"));
    final KsqlConfig ksqlConfigClone = ksqlConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test-2"));
    String result = ksqlConfigClone.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
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

  @Test
  public void shouldPreserveOriginalCompatibilitySensitiveConfigs() {
    Map<String, String> originalProperties = ImmutableMap.of(
        KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "not_the_default");
    KsqlConfig currentConfig = new KsqlConfig(Collections.emptyMap());
    KsqlConfig compatibleConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(originalProperties);
    assertThat(
        compatibleConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG),
        equalTo("not_the_default"));
  }

  @Test
  public void shouldUseCurrentValueForCompatibilityInsensitiveConfigs() {
    Map<String, String> originalProperties = Collections.singletonMap(KsqlConfig.KSQL_ENABLE_UDFS, "false");
    KsqlConfig currentConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.KSQL_ENABLE_UDFS, true));
    KsqlConfig compatibleConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(originalProperties);
    assertThat(compatibleConfig.getBoolean(KsqlConfig.KSQL_ENABLE_UDFS), is(true));
  }
}
