/*
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.errors.LogMetricAndContinueExceptionHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class KsqlConfigTest {

  @Test
  public void shouldSetInitialValuesCorrectly() {
    final Map<String, Object> initialProps = new HashMap<>();
    initialProps.put(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 10);
    initialProps.put(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 3);
    initialProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 800);
    initialProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

    final KsqlConfig ksqlConfig = new KsqlConfig(initialProps);

    assertThat(ksqlConfig.getInt(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY), equalTo(10));
    assertThat(ksqlConfig.getShort(KsqlConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY), equalTo((short) 3));

  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerByDefault() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, IsEqual.equalTo(LogMetricAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldSetLogAndContinueExceptionHandlerWhenFailOnDeserializationErrorFalse() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, false));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, IsEqual.equalTo(LogMetricAndContinueExceptionHandler.class));
  }

  @Test
  public void shouldNotSetDeserializationExceptionHandlerWhenFailOnDeserializationErrorTrue() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG, true));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG);
    assertThat(result, nullValue());
  }

  @Test
  public void shouldSetStreamsConfigConsumerUnprefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    final Object result = ksqlConfig.getKsqlStreamConfigProps().get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    assertThat(result, equalTo("earliest"));
  }

  @Test
  public void shouldSetStreamsConfigConsumerPrefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
        .get(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        equalTo(100));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigConsumerKsqlPrefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        equalTo(100));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(nullValue()));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG),
        is(nullValue()));
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

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
        .get(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        equalTo(1024L));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(nullValue()));
  }

  @Test
  public void shouldSetStreamsConfigKsqlProducerPrefixedProperties() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG, "1024"));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        equalTo(1024L));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(nullValue()));

    assertThat(ksqlConfig.getKsqlStreamConfigProps()
            .get(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PRODUCER_PREFIX + ProducerConfig.BUFFER_MEMORY_CONFIG),
        is(nullValue()));
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

    assertThat(ksqlConfig.getKsqlStreamConfigProps().
        get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG), equalTo(128L));

    assertThat(ksqlConfig.getKsqlStreamConfigProps().
        get(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG),
        is(nullValue()));
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
    final String result = ksqlConfigClone.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    assertThat(result, equalTo("test-2"));
  }

  @Test
  public void shouldCloneWithStreamPropertyOverwrite() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));
    final KsqlConfig ksqlConfigClone = ksqlConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "200"));
    final Object result = ksqlConfigClone.getKsqlStreamConfigProps().get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
    assertThat(result, equalTo(200));
  }

  @Test
  public void shouldCloneWithPrefixedStreamPropertyOverwrite() {
    final KsqlConfig ksqlConfig = new KsqlConfig(Collections.singletonMap(
        KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "100"));
    final KsqlConfig ksqlConfigClone = ksqlConfig.cloneWithPropertyOverwrite(
        Collections.singletonMap(
            KsqlConfig.KSQL_STREAMS_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "200"));
    final Object result = ksqlConfigClone.getKsqlStreamConfigProps().get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
    assertThat(result, equalTo(200));
  }

  @Test
  public void shouldPreserveOriginalCompatibilitySensitiveConfigs() {
    final Map<String, String> originalProperties = ImmutableMap.of(
        KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, "not_the_default");
    final KsqlConfig currentConfig = new KsqlConfig(Collections.emptyMap());
    final KsqlConfig compatibleConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(originalProperties);
    assertThat(
        compatibleConfig.getString(KsqlConfig.KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG),
        equalTo("not_the_default"));
  }

  @Test
  public void shouldUseCurrentValueForCompatibilityInsensitiveConfigs() {
    final Map<String, String> originalProperties = Collections.singletonMap(KsqlConfig.KSQL_ENABLE_UDFS, "false");
    final KsqlConfig currentConfig = new KsqlConfig(Collections.singletonMap(KsqlConfig.KSQL_ENABLE_UDFS, true));
    final KsqlConfig compatibleConfig = currentConfig.overrideBreakingConfigsWithOriginalValues(originalProperties);
    assertThat(compatibleConfig.getBoolean(KsqlConfig.KSQL_ENABLE_UDFS), is(true));
  }

  @Test
  public void shouldReturnUdfConfig() {
    // Given:
    final String functionName = "bob";

    final String udfConfigName =
        KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + functionName + ".some-setting";

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        udfConfigName, "should-be-visible"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKsqlFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.get(udfConfigName), is("should-be-visible"));
  }

  @Test
  public void shouldReturnUdfConfigOnlyIfLowercase() {
    // Given:
    final String functionName = "BOB";

    final String correctConfigName =
        KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + functionName.toLowerCase() + ".some-setting";

    final String invalidConfigName =
        KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + functionName + ".some-other-setting";

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        invalidConfigName, "should-not-be-visible",
        correctConfigName, "should-be-visible"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKsqlFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.keySet(), contains(correctConfigName));
  }

  @Test
  public void shouldReturnGlobalUdfConfig() {
    // Given:
    final String globalConfigName =
        KsqlConfig.KSQ_FUNCTIONS_GLOBAL_PROPERTY_PREFIX + ".some-setting";

    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        globalConfigName, "global"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKsqlFunctionsConfigProps("what-eva");

    // Then:
    assertThat(udfProps.get(globalConfigName), is("global"));
  }

  @Test
  public void shouldNotReturnNoneUdfConfig() {
    // Given:
    final String functionName = "bob";
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "not a udf property",
        KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + "different_udf.some-setting", "different udf property"
    ));

    // When:
    final Map<String, ?> udfProps = config.getKsqlFunctionsConfigProps(functionName);

    // Then:
    assertThat(udfProps.keySet(), is(empty()));
  }

  @Test
  public void shouldListKnownKsqlConfig() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_SERVICE_ID_CONFIG, "not sensitive",
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "sensitive!"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KsqlConfig.KSQL_SERVICE_ID_CONFIG), is("not sensitive"));
  }

  @Test
  public void shouldListKnownKsqlFunctionConfig() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG, "true"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KsqlConfig.KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG), is("true"));
  }

  @Test
  public void shouldListUnknownKsqlFunctionConfigObfuscated() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop", "maybe sensitive"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KsqlConfig.KSQ_FUNCTIONS_PROPERTY_PREFIX + "some_udf.some.prop"),
        is("[hidden]"));
  }

  @Test
  public void shouldListKnownStreamsConfigObfuscated() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "not sensitive",
        KsqlConfig.KSQL_STREAMS_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "sensitive!",
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX +
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "sensitive!"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.APPLICATION_ID_CONFIG),
        is("not sensitive"));
    assertThat(result.get(
        KsqlConfig.KSQL_STREAMS_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
        is("[hidden]"));
    assertThat(result.get(KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.CONSUMER_PREFIX
            + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
        is("[hidden]"));
  }

  @Test
  public void shouldNotListUnresolvedServerConfig() {
    // Given:
    final KsqlConfig config = new KsqlConfig(ImmutableMap.of(
        "some.random.property", "might be sensitive"
    ));

    // When:
    final Map<String, String> result = config.getAllConfigPropsWithSecretsObfuscated();

    // Then:
    assertThat(result.get("some.random.property"), is(nullValue()));
  }
}
