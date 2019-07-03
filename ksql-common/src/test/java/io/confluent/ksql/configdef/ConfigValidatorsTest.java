/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.configdef;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ConfigValidatorsTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFailIfValueNoInEnum() {
    // Given:
    final Validator validator = ConfigValidators.enumValues(TestEnum.class);

    // Then:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("String must be one of: FOO, BAR");

    // When:
    validator.ensureValid("propName", "NotValid");
  }

  @Test
  public void shouldNotThrowIfAValidEnumValue() {
    // Given:
    final Validator validator = ConfigValidators.enumValues(TestEnum.class);

    // When:
    validator.ensureValid("propName", TestEnum.FOO.toString());

    // Then: did not throw
  }

  @Test
  public void shouldNotThrowIfAValidEnumValueInDifferentCase() {
    // Given:
    final Validator validator = ConfigValidators.enumValues(TestEnum.class);

    // When:
    validator.ensureValid("propName", TestEnum.FOO.toString().toLowerCase());

    // Then: did not throw
  }

  @Test
  public void shouldNotThrowifMatchForCaseInsensitiveString() {
    // Given:
    final Validator validator = ConfigValidators.ValidCaseInsensitiveString.in("a", "B");

    // When:
    validator.ensureValid("propName", "A");

    // Then: did not throw
  }

  @Test
  public void shouldThrowIfNoMatchForCaseInsensitiveString() {
    // Given:
    final Validator validator = ConfigValidators.ValidCaseInsensitiveString.in("a", "B");

    // Then:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value c for configuration propName: "
        + "String must be one of: A, B");

    // When:
    validator.ensureValid("propName", "c");
  }

  @Test
  public void shouldNotThrowIfMatchForCaseInsensitiveStringIncludesNull() {
    // Given:
    final Validator validator = ConfigValidators.ValidCaseInsensitiveString.in("a", "B", null);

    // When:
    validator.ensureValid("propName", null);

    // Then: did not throw
  }

  @Test
  public void shouldThrowIfNoMatchForCaseInsensitiveStringNull() {
    // Given:
    final Validator validator = ConfigValidators.ValidCaseInsensitiveString.in("a", "B");

    // Then:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage("Invalid value null for configuration propName: "
        + "String must be one of: A, B");

    // When:
    validator.ensureValid("propName", null);
  }

  private enum TestEnum {
    FOO, BAR
  }
}