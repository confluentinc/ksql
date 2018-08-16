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

package io.confluent.ksql.function.udf.string;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

public class SubstringTest {

  private Substring udf;

  @Before
  public void setUp() {
    udf = new Substring();
  }

  @Test
  public void shouldReturnNullOnNullValue() {
    assertThat(udf.substring(null, 1), is(nullValue()));
    assertThat(udf.substring(null, 1, 1), is(nullValue()));
    assertThat(udf.substring("some string", null, 1), is(nullValue()));
    assertThat(udf.substring("some string", 1, null), is(nullValue()));
  }

  @Test
  public void shouldUseOneBasedIndexing() {
    assertThat(udf.substring("a test string", 1, 1), is("a"));
    assertThat(udf.substring("a test string", -1, 1), is("g"));
  }

  @Test
  public void shouldExtractFromStartForPositivePositions() {
    assertThat(udf.substring("a test string", 3), is("test string"));
    assertThat(udf.substring("a test string", 3, 4), is("test"));
  }

  @Test
  public void shouldExtractFromEndForNegativePositions() {
    assertThat(udf.substring("a test string", -6), is("string"));
    assertThat(udf.substring("a test string", -6, 2), is("st"));
  }

  @Test
  public void shouldTruncateOutOfBoundIndexes() {
    assertThat(udf.substring("a test string", 0), is("a test string"));
    assertThat(udf.substring("a test string", 100), is(""));
    assertThat(udf.substring("a test string", -100), is("a test string"));
    assertThat(udf.substring("a test string", 3, 100), is("test string"));
    assertThat(udf.substring("a test string", 3, -100), is(""));
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNPEOnNullValueWithTwoArgs() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring(null, 0);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNPEOnNullValueWithThreeArgs() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring(null, 0, 0);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNPEOnNullStartIndexWithTwoArgs() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring("some-string", null);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNPEOnNullStartIndexWithThreeArgs() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring("some-string", null, 0);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNPEOnNullEndIndex() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring("some-string", 1, null);
  }

  @Test
  public void shouldUseZeroBasedIndexingIfInLegacyMode() {
    // Given:
    givenInLegacyMode();

    // Then:
    assertThat(udf.substring("a test string", 0, 1), is("a"));
  }

  @Test(expected = StringIndexOutOfBoundsException.class)
  public void shouldThrowInLegacyModeIfStartIndexIsNegative() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring("a test string", -1, 1);
  }

  @Test
  public void shouldExtractFromStartInLegacyMode() {
    // Given:
    givenInLegacyMode();

    // Then:
    assertThat(udf.substring("a test string", 2), is("test string"));
    assertThat(udf.substring("a test string", 2, 6), is("test"));
  }

  @Test(expected = StringIndexOutOfBoundsException.class)
  public void shouldThrowInLegacyModeIfEndIndexIsLessThanStartIndex() {
    // Given:
    givenInLegacyMode();

    // Then:
    assertThat(udf.substring("a test string", 4, 2), is("st"));
  }

  @Test(expected = StringIndexOutOfBoundsException.class)
  public void shouldThrowInLegacyModeIfStartIndexOutOfBounds() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring("a test string", 100);
  }

  @Test(expected = StringIndexOutOfBoundsException.class)
  public void shouldThrowInLegacyModeIfEndIndexOutOfBounds() {
    // Given:
    givenInLegacyMode();

    // Then:
    udf.substring("a test string", 3, 100);
  }

  @Test
  public void shouldNotEnterLegacyModeIfConfigMissing() {
    // When:
    udf.configure(ImmutableMap.of());

    // Then:
    assertThat(udfIsInLegacyMode(), is(false));
  }

  @Test
  public void shouldEnterLegacyModeWithTrueStringConfig() {
    // When:
    configure("true");

    // Then:
    assertThat(udfIsInLegacyMode(), is(true));
  }

  @Test
  public void shouldEnterLegacyModeWithTrueBooleanConfig() {
    // When:
    configure(true);

    // Then:
    assertThat(udfIsInLegacyMode(), is(true));
  }

  @Test
  public void shouldNotEnterLegacyModeWithFalseStringConfig() {
    // When:
    configure("false");

    // Then:
    assertThat(udfIsInLegacyMode(), is(false));
  }

  @Test
  public void shouldNotEnterLegacyModeWithFalseBooleanConfig() {
    // When:
    configure(false);

    // Then:
    assertThat(udfIsInLegacyMode(), is(false));
  }

  @Test(expected = ConfigException.class)
  public void shouldThrowOnInvalidLegacyModeValueType() {
    configure(1.0);
  }

  private boolean udfIsInLegacyMode() {
    // In legacy mode an NPE is thrown on null args:
    try {
      udf.substring(null, null);
      return false;
    } catch (final NullPointerException e) {
      return true;
    }
  }

  private void givenInLegacyMode() {
    configure(true);
  }

  private void configure(final Object legacyMode) {
    udf.configure(ImmutableMap.of(KsqlConfig.KSQL_FUNCTIONS_SUBSTRING_LEGACY_ARGS_CONFIG, legacyMode));
  }
}