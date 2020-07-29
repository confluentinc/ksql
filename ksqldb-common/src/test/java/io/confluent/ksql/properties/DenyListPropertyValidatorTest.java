/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.properties;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

public class DenyListPropertyValidatorTest {
  private DenyListPropertyValidator validator;

  @Before
  public void setUp() {
    validator = new DenyListPropertyValidator(Arrays.asList(
        "immutable-property"
    ));
  }

  @Test
  public void shouldThrowOnDenyListedProperty() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> validator.validateAll(ImmutableMap.of(
            "immutable-property", "v1",
            "anything", "v2"
        ))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "A property override was set locally for a property that the server prohibits "
            + "overrides for: 'immutable-property'"
    ));

  }

  @Test
  public void shouldNotThrowOnConfigurableProp() {
    validator.validateAll(ImmutableMap.of(
        "mutable-1", "v1",
        "anything", "v2"
    ));
  }
}
