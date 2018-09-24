/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.config;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigItemTest {

  private static final ConfigKey KEY_NO_VALIDATOR = KsqlConfig.CURRENT_DEF.configKeys()
      .get(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);

  private static final ConfigKey KEY_WITH_VALIDATOR = StreamsConfig.configDef().configKeys()
      .get(StreamsConfig.SEND_BUFFER_CONFIG);

  private static final ConfigKey PASSWORD_KEY = KsqlConfig.CURRENT_DEF.configKeys()
      .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);

  private static final ConfigItem RESOLVED_NO_VALIDATOR = ConfigItem.resolved(KEY_NO_VALIDATOR);
  private static final ConfigItem RESOLVED_WITH_VALIDATOR = ConfigItem.resolved(KEY_WITH_VALIDATOR);
  private static final ConfigItem RESOLVED_PASSWORD = ConfigItem.resolved(PASSWORD_KEY);
  private static final ConfigItem UNRESOLVED = ConfigItem.unresolved("some.unresolved.prop");

  @BeforeClass
  public static void classSetUp() {
    assertThat("Invalid test", KEY_NO_VALIDATOR.validator, is(nullValue()));
    assertThat("Invalid test", KEY_WITH_VALIDATOR.validator, is(notNullValue()));
    assertThat("Invalid test", PASSWORD_KEY.type, is(Type.PASSWORD));
  }

  @Test
  public void shouldReturnPropertyName() {
    assertThat(RESOLVED_NO_VALIDATOR.getPropertyName(),
        is(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));
    assertThat(RESOLVED_WITH_VALIDATOR.getPropertyName(), is(StreamsConfig.SEND_BUFFER_CONFIG));
    assertThat(UNRESOLVED.getPropertyName(), is("some.unresolved.prop"));
  }

  @Test
  public void shouldPassThroughUnresolvedValueOnParse() {
    assertThat(UNRESOLVED.parseValue("anything"), is("anything"));
    assertThat(UNRESOLVED.parseValue(12345L), is(12345L));
  }

  @Test
  public void shouldParseResolvedValue() {
    assertThat(RESOLVED_NO_VALIDATOR.parseValue(101), is(101));
  }

  @Test
  public void shouldCoerceParsedValue() {
    assertThat(RESOLVED_NO_VALIDATOR.parseValue("101"), is(101));
  }

  @Test
  public void shouldValidateParsedValue() {
    assertThat(RESOLVED_WITH_VALIDATOR.parseValue(101), is(101));
  }

  @Test(expected = ConfigException.class)
  public void shouldThrowIfCanNotCoerceParsedValue() {
    RESOLVED_WITH_VALIDATOR.parseValue("not a number");
  }

  @Test
  public void shouldPassThroughUnresolvedOnConvertToString() {
    assertThat(UNRESOLVED.convertToString(12345L), is("12345"));
    assertThat(UNRESOLVED.convertToString(null), is("NULL"));
  }

  @Test
  public void shouldConvertResolvedToString() {
    assertThat(RESOLVED_NO_VALIDATOR.convertToString("101"), is("101"));
  }

  @Test
  public void shouldObfuscatePasswordsOnResolveToString() {
    assertThat(RESOLVED_PASSWORD.convertToString("Sensitive"), is("[hidden]"));
  }
}