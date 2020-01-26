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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfigValidatorsTest {

  @Mock
  private Function<String, ?> parser;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldFailIfValueNotInEnum() {
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
  public void shouldNotThrowIfEnumValueIsNull() {
    // Given:
    final Validator validator = ConfigValidators.enumValues(TestEnum.class);

    // When:
    validator.ensureValid("propName", null);

    // Then: did not throw
  }

  @Test
  public void shouldNotThrowIfMatchForCaseInsensitiveString() {
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

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNonStringFromParse() {
    // Given:
    final Validator validator = ConfigValidators.parses(parser);

    // When:
    validator.ensureValid("propName", 10);
  }

  @Test
  public void shouldPassNullsToParser() {
    // Given:
    final Validator validator = ConfigValidators.parses(parser);
    
    // When:
    validator.ensureValid("propName", null);
    
    // Then:
    verify(parser).apply(null);
  }

  @Test
  public void shouldPassStringsToParser() {
    // Given:
    final Validator validator = ConfigValidators.parses(parser);
    
    // When:
    validator.ensureValid("propName", "value");

    // Then:
    verify(parser).apply("value");
  }

  @Test
  public void shouldThrowIfParserThrows() {
    // Given:
    final Validator validator = ConfigValidators.parses(parser);
    when(parser.apply(any())).thenThrow(new IllegalArgumentException("some error"));

    // Then:
    expectedException.expect(ConfigException.class);
    expectedException
            .expectMessage("Configuration propName is invalid: some error");

    // When:
    validator.ensureValid("propName", "value");
  }

  @Test
  public void shouldThrowOnInvalidURL() {
    // Given:
    final Validator validator = ConfigValidators.validUrl();

    // Then:
    expectedException.expect(ConfigException.class);
    expectedException.expectMessage(
            "Invalid value INVALID for configuration propName: Not valid URL: no protocol: INVALID");

    // When:
    validator.ensureValid("propName", "INVALID");
  }

  @Test
  public void shouldNotThrowOnValidURL() {
    // Given:
    final Validator validator = ConfigValidators.validUrl();

    // When:
    validator.ensureValid("propName", "http://valid:25896/somePath");

    // Then: did not throw.
  }

  private enum TestEnum {
    FOO, BAR
  }
}