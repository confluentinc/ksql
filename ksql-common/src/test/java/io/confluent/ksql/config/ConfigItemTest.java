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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

public class ConfigItemTest {
  private static final ConfigDef KSQL_CONFIG_DEF = KsqlConfig.CURRENT_DEF;
  private ConfigItem config;

  @Before
  public void setUp() {
    config = new ConfigItem(KSQL_CONFIG_DEF, KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY);
  }

  @Test
  public void shouldThrowOnUnknownProp() {
    new ConfigItem(KSQL_CONFIG_DEF, "You won't find me");
  }

  @Test
  public void shouldReturnPropertyName() {
    assertThat(config.getPropertyName(), is(KsqlConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY));
  }

  @Test
  public void shouldReturnDef() {
    assertThat(config.getDef(), is(sameInstance(KSQL_CONFIG_DEF)));
  }

  @Test
  public void shouldParseValidValue() {
    assertThat(config.parseValue(101), is(101));
  }

  @Test
  public void shouldCoerceParsedValue() {
    assertThat(config.parseValue("101"), is(101));
  }

  @Test(expected = ConfigException.class)
  public void shouldThrowIfCanNotCoerceParsedValue() {
    config.parseValue("not a number");
  }
}