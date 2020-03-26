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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Test;

public class LocalPropertyValidatorTest {

  private static final Collection<String> IMMUTABLE_PROPS =
      ImmutableList.of("immutable-1", "immutable-2");

  private LocalPropertyValidator validator;

  @Before
  public void setUp() {
    validator = new LocalPropertyValidator(IMMUTABLE_PROPS);
  }

  @Test
  public void shouldThrowOnImmutableProp() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> validator.validate("immutable-2", "anything"));
    assertThat(e.getMessage(), containsString("Cannot override property 'immutable-2'"));
  }

  @Test
  public void shouldNotThrowOnMutableProp() {
    validator.validate("mutable-1", "anything");
  }

  @Test
  public void shouldThrowOnNoneOffsetReset() {
    Exception e = assertThrows(IllegalArgumentException.class,
        () -> validator.validate(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none"));
    assertThat(e.getMessage(), containsString("'none' is not valid for this property within KSQL"));
  }

  @Test
  public void shouldNotThrowOnOtherOffsetReset() {
    validator.validate(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "caught-by-normal-mech");
  }
}