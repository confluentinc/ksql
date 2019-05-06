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

package io.confluent.ksql.config;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConfigItemTest {

  private static final ConfigKey KEY_NO_VALIDATOR = KsqlConfig.CURRENT_DEF.configKeys()
      .get(KsqlConfig.KSQL_SERVICE_ID_CONFIG);

  private static final ConfigKey KEY_WITH_VALIDATOR = StreamsConfig.configDef().configKeys()
      .get(StreamsConfig.SEND_BUFFER_CONFIG);

  private static final ConfigKey KEY_NO_DEFAULT = StreamsConfig.configDef().configKeys()
      .get(StreamsConfig.APPLICATION_ID_CONFIG);

  private static final ConfigKey PASSWORD_KEY = KsqlConfig.CURRENT_DEF.configKeys()
      .get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);

  private static final ConfigItem RESOLVED_NO_VALIDATOR = ConfigItem.resolved(KEY_NO_VALIDATOR);
  private static final ConfigItem RESOLVED_WITH_VALIDATOR = ConfigItem.resolved(KEY_WITH_VALIDATOR);
  private static final ConfigItem RESOLVED_NO_DEFAULT = ConfigItem.resolved(KEY_NO_DEFAULT);
  private static final ConfigItem RESOLVED_PASSWORD = ConfigItem.resolved(PASSWORD_KEY);
  private static final ConfigItem UNRESOLVED = ConfigItem.unresolved("some.unresolved.prop");

  @BeforeClass
  public static void classSetUp() {
    assertThat("Invalid test", KEY_NO_VALIDATOR.validator, is(nullValue()));
    assertThat("Invalid test", KEY_WITH_VALIDATOR.validator, is(notNullValue()));
    assertThat("Invalid test", KEY_NO_DEFAULT.defaultValue, is(ConfigDef.NO_DEFAULT_VALUE));
    assertThat("Invalid test", PASSWORD_KEY.type, is(Type.PASSWORD));
  }

  @Test
  public void shouldReturnResolved() {
    assertThat(RESOLVED_NO_VALIDATOR.isResolved(), is(true));
    assertThat(UNRESOLVED.isResolved(), is(false));
  }

  @Test
  public void shouldReturnPropertyName() {
    assertThat(RESOLVED_NO_VALIDATOR.getPropertyName(),
        is(KsqlConfig.KSQL_SERVICE_ID_CONFIG));
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
    assertThat(RESOLVED_NO_VALIDATOR.parseValue("ksql_default"), is("ksql_default"));
  }

  @Test
  public void shouldCoerceParsedValue() {
    assertThat(RESOLVED_NO_VALIDATOR.parseValue("ksql_default"), is("ksql_default"));
  }

  @Test
  public void shouldResolvePasswordToPassword() {
    assertThat(RESOLVED_PASSWORD.parseValue("Sensitive"), is(new Password("Sensitive")));
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

  @Test
  public void shouldNotBeDefaultValueIfNotResolved() {
    assertThat(UNRESOLVED.isDefaultValue("anything"), is(false));
    assertThat(UNRESOLVED.isDefaultValue(null), is(false));
  }

  @Test
  public void shouldBeDefaultValue() {
    assertThat(RESOLVED_NO_VALIDATOR.isDefaultValue("default_"),
        is(true));
  }

  @Test
  public void shouldCoerceBeforeCheckingIfDefaultValue() {
    assertThat(RESOLVED_NO_VALIDATOR
            .isDefaultValue("default_"), is(true));
  }

  @Test
  public void shouldHandleNoDefaultValue() {
    assertThat(RESOLVED_NO_DEFAULT.isDefaultValue("anything"), is(false));
  }

  @Test
  public void shouldHandlePasswordDefaultValue() {
    assertThat(RESOLVED_PASSWORD.isDefaultValue("anything"), is(false));
  }
}