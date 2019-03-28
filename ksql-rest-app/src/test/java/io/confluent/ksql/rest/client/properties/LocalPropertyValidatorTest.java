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

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LocalPropertyValidatorTest {

  private static final Collection<String> IMMUTABLE_PROPS =
      ImmutableList.of("immutable-1", "immutable-2");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private LocalPropertyValidator validator;

  @Before
  public void setUp() {
    validator = new LocalPropertyValidator();
  }

  @Test
  public void shouldThrowOnImmutableProp() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot override property 'immutable-2'");

    validator.validate("immutable-2", "anything");
  }

  @Test
  public void shouldNotThrowOnMutableProp() {
    validator.validate("mutable-1", "anything");
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