/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import static com.google.common.collect.ImmutableMap.of;
import static io.confluent.ksql.rest.util.RocksDBConfigSetterHandler.maybeConfigureRocksDBConfigSetter;
import static java.lang.Class.forName;
import static org.apache.kafka.streams.StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.Options;

@RunWith(MockitoJUnitRunner.class)
public class RocksDBConfigSetterHandlerTest {

  @Mock
  private KsqlConfig ksqlConfig;

  @Test
  public void shouldConfigure() throws Exception {
    // Given:
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(
        ImmutableMap.of(
            StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
            Class.forName("io.confluent.ksql.rest.util.RocksDBConfigSetterHandlerTest$ConfigurableTestRocksDBConfigSetter"))
    );
    final Runnable mockRunnable = mock(Runnable.class);
    when(ksqlConfig.originals()).thenReturn(
        ImmutableMap.of(ConfigurableTestRocksDBConfigSetter.TEST_CONFIG, mockRunnable));

    // When:
    RocksDBConfigSetterHandler.maybeConfigureRocksDBConfigSetter(ksqlConfig);

    // Then:
    verify(mockRunnable).run();
  }

  @Test
  public void shouldStartWithNonConfigurableRocksDBConfigSetter() throws Exception {
    // Given:
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(
        ImmutableMap.of(
            StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
            Class.forName("io.confluent.ksql.rest.util.RocksDBConfigSetterHandlerTest$NonConfigurableTestRocksDBConfigSetter"))
    );

    // No error when:
    RocksDBConfigSetterHandler.maybeConfigureRocksDBConfigSetter(ksqlConfig);
  }

  @Test
  public void shouldThrowIfFailToRocksDBConfigSetter() throws Exception {
    // Given:
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(
        of(
            ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
            forName("io.confluent.ksql.rest.util.RocksDBConfigSetterHandlerTest$ConfigurableTestRocksDBConfigSetterWithoutPublicConstructor"))
    );

    // When
    final ConfigException e = assertThrows(
        ConfigException.class,
        () -> maybeConfigureRocksDBConfigSetter(ksqlConfig)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Failed to configure Configurable RocksDBConfigSetter."));
    assertThat(e.getMessage(), containsString(ROCKSDB_CONFIG_SETTER_CLASS_CONFIG));
    assertThat(e.getMessage(), containsString(
        "io.confluent.ksql.rest.util.RocksDBConfigSetterHandlerTest"
            + "$ConfigurableTestRocksDBConfigSetterWithoutPublicConstructor")
    );
  }

  public static class ConfigurableTestRocksDBConfigSetter
      extends NonConfigurableTestRocksDBConfigSetter
      implements org.apache.kafka.common.Configurable {

    static final String TEST_CONFIG = "test.runnable";

    @Override
    public void configure(final Map<String, ?> config) {
      final Runnable supplier = (Runnable) config.get(TEST_CONFIG);
      supplier.run();
    }
  }

  static class ConfigurableTestRocksDBConfigSetterWithoutPublicConstructor
      extends NonConfigurableTestRocksDBConfigSetter
      implements org.apache.kafka.common.Configurable {

    @Override
    public void configure(final Map<String, ?> config) {
    }
  }

  private static class NonConfigurableTestRocksDBConfigSetter implements RocksDBConfigSetter {

    @Override
    public void setConfig(
        final String storeName,
        final Options options,
        final Map<String, Object> configs) {
      // do nothing
    }

    @Override
    public void close(final String storeName, final Options options) {
    }
  }
}