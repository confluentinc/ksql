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

package io.confluent.ksql.rest.client.properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LocalPropertyValidatorTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private LocalPropertyValidator validator;

  @Before
  public void setUp() {
    validator = new LocalPropertyValidator();
  }

  @Test
  public void shouldThrowOnNonConfigurableProp() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot override property 'foo'");

    validator.validate("foo", "anything");
  }

  @Test
  public void shouldNotThrowOnConfigurableProp() {
    LocalPropertyValidator
        .CONFIG_PROPERTY_WHITELIST
        .forEach(s -> validator.validate(s, "anything"));
  }

  @Test
  public void shouldThrowOnNoneOffsetReset() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("'none' is not valid for this property within KSQL");

    validator.validate(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
  }

  @Test
  public void shouldNotThrowOnOtherOffsetReset() {
    validator.validate(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "caught-by-normal-mech");
  }
}