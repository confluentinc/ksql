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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfigValidatorsTest {

  @Mock
  private Function<String, ?> parser;

  @Test
  public void shouldFailIfValueNotInEnum() {
    // Given:
    final Validator validator = ConfigValidators.enumValues(TestEnum.class);

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "NotValid")
    );

    // Then:
    assertThat(e.getMessage(), containsString("String must be one of: FOO, BAR"));
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

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "c")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid value c for configuration propName: "
        + "String must be one of: A, B"));
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

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", null)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid value null for configuration propName: "
        + "String must be one of: A, B"));
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

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "value")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Configuration propName is invalid: some error"));
  }

  @Test
  public void shouldThrowOnInvalidURL() {
    // Given:
    final Validator validator = ConfigValidators.validUrl();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "INVALID")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Invalid value INVALID for configuration propName: Not valid URL: no protocol: INVALID"));
  }

  @Test
  public void shouldNotThrowOnValidURL() {
    // Given:
    final Validator validator = ConfigValidators.validUrl();

    // When:
    validator.ensureValid("propName", "http://valid:25896/somePath");

    // Then: did not throw.
  }

  @Test
  public void shouldNotThrowOnValidRegex() {
    // Given
    final Validator validator = ConfigValidators.validRegex();

    // When:
    validator.ensureValid("propName", Collections.singletonList("prefix_.*"));

    // Then: did not throw.
  }

  @Test
  public void shouldThrowOnInvalidRegex() {
    // Given:
    final Validator validator = ConfigValidators.validRegex();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", Collections.singletonList("*_suffix"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Not valid regular expression: "));
  }

  @Test
  public void shouldThrowOnNoRegexList() {
    // Given:
    final Validator validator = ConfigValidators.validRegex();

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> validator.ensureValid("propName", "*.*")
    );

    // Then:
    assertThat(e.getMessage(), containsString("validator should only be used with LIST of STRING defs"));
  }

  @Test
  public void shouldThrowOnNoStringRegexList() {
    // Given:
    final Validator validator = ConfigValidators.validRegex();

    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> validator.ensureValid("propName", Collections.singletonList(1))
    );

    // Then:
    assertThat(e.getMessage(), containsString("validator should only be used with LIST of STRING defs"));
  }

  @Test
  public void shouldParseDoubleValueInMap() {
    // Given:
    final Validator validator = ConfigValidators.mapWithDoubleValue();
    validator.ensureValid("propName", "foo:1.2,bar:3");
  }

  @Test
  public void shouldParseIntKeyDoubleValueInMap() {
    // Given:
    final Validator validator = ConfigValidators.mapWithIntKeyDoubleValue();
    validator.ensureValid("propName", "123:1.2,345:9.0");
  }

  @Test
  public void shouldThrowOnBadDoubleValueInMap() {
    // Given:
    final Validator validator = ConfigValidators.mapWithDoubleValue();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "foo:abc")
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Invalid value abc for configuration propName: Not a double"));
  }

  @Test
  public void shouldThrowOnBadIntDoubleValueInMap() {
    // Given:
    final Validator validator = ConfigValidators.mapWithIntKeyDoubleValue();

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "1:abc")
    );
    final Exception e2 = assertThrows(
        ConfigException.class,
        () -> validator.ensureValid("propName", "abc:1.2")
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Invalid value abc for configuration propName: Not a double"));
    assertThat(e2.getMessage(),
        containsString("Invalid value abc for configuration propName: Not an int"));
  }

  private enum TestEnum {
    FOO, BAR
  }
}